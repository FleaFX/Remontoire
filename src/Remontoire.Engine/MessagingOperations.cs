using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

/// <summary>
/// Publish, ack, nack and consume against a group this process hosts — the one implementation behind the
/// gRPC client endpoint and the embedded producer and consumer, so both behave the same. Resolving
/// the group, and answering when it isn't hosted here, stays with the caller.
/// </summary>
/// <param name="assignmentTable">The source of every stream's consumer-group policies.</param>
/// <param name="metrics">Records the published, acknowledged, retried and parked messages.</param>
/// <param name="logger">Logs every parked message.</param>
public sealed class MessagingOperations(ShardAssignmentTable assignmentTable, RemontoireMetrics metrics, ILogger<MessagingOperations> logger) {
    /// <summary>
    /// The header a message's trace context rides under, in W3C <c>traceparent</c> format: stored
    /// with the message at publish, so every later receive of it can link back to the publish.
    /// </summary>
    public const string TraceParentHeaderKey = "traceparent";

    /// <summary>
    /// The most characters of a nack's reason that are kept.
    /// </summary>
    public const int MaxReasonLength = 1024;

    static readonly TimeSpan NackApplyTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Proposes one message to <paramref name="replica"/> in a <c>publish</c> span and completes
    /// once it is committed. Unless the caller supplied one, the current trace context is stored
    /// as the message's <c>traceparent</c> header.
    /// </summary>
    /// <param name="streamName">The stream, for the metrics.</param>
    /// <param name="replica">The replica of the group serving the message's virtual shard.</param>
    /// <param name="partitionKey">The UTF-8 partition key.</param>
    /// <param name="headers">The message's headers.</param>
    /// <param name="payload">The message's payload.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The committed offset and the leader's ingest timestamp.</returns>
    /// <exception cref="NotLeaderException"><paramref name="replica"/> doesn't lead its group.</exception>
    public async Task<ProposeResult> PublishAsync(
        string streamName,
        RaftReplica replica,
        ReadOnlyMemory<byte> partitionKey,
        IEnumerable<Header> headers,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken
    ) {
        // Current from here on: the propose below captures it as the parent of its Raft spans, and
        // its context is the one stored. Without a listener, the caller's own context (if any) is
        // stored instead.
        using var activity = RemontoireActivitySource.Source.StartActivity($"publish {streamName}", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "remontoire")
            .SetTag("messaging.operation.type", "send")
            .SetTag("messaging.operation.name", "publish")
            .SetTag("messaging.destination.name", streamName);

        var storedHeaders = headers.ToList();
        if (Activity.Current?.Id is { } traceparent && !storedHeaders.Any(header => HeaderKeyEquals(header, TraceParentHeaderKey)))
            storedHeaders.Add(new Header(Encoding.UTF8.GetBytes(TraceParentHeaderKey), Encoding.UTF8.GetBytes(traceparent)));

        var result = await replica.ProposeAsync(new AppendRequest(partitionKey, storedHeaders, payload), cancellationToken);
        metrics.RecordPublished(streamName, replica.GroupId);
        return result;
    }

    /// <summary>
    /// Acknowledges <paramref name="offsets"/> for <paramref name="consumerGroup"/> the way the
    /// group's ack mode asks: through Raft in strict mode, locally on the leader in checkpoint mode.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <param name="offsets">The acknowledged offsets.</param>
    /// <param name="replica">The replica of the group serving the virtual shard.</param>
    /// <param name="shardLog">That group's shard log.</param>
    /// <param name="ackIndex">That group's ack index.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <exception cref="NotLeaderException"><paramref name="replica"/> doesn't lead its group.</exception>
    public async Task AckAsync(
        string streamName,
        string consumerGroup,
        IReadOnlyList<ulong> offsets,
        RaftReplica replica,
        ShardLog shardLog,
        AckIndex ackIndex,
        CancellationToken cancellationToken
    ) {
        if (assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup).Mode == AckMode.Checkpoint) {
            // Today's leader-pinning for Ack is an accidental side effect of always calling
            // ProposeAsync (which throws NotLeaderException off a non-ready-leader replica). That
            // side effect disappears the moment this path skips ProposeAsync entirely — this check
            // is now load-bearing, not incidental: a checkpoint-mode ack applied on a follower would
            // silently vanish at that follower's own next crash, before the leader's own periodic
            // checkpoint (AckCheckpointer) ever captures it.
            if (!replica.IsLeader)
                throw new NotLeaderException(replica.GroupId, replica.LeaderHint);

            // Checkpoint mode skips Raft entirely (that's the whole point), so nothing else ever
            // validates these client-supplied offsets — unlike the strict path below, forging one
            // here costs no round-trip. Silently drop anything referring to a message that doesn't
            // exist yet rather than letting it advance this group's watermark past undelivered data.
            await ackIndex.ApplyLocalAsync(consumerGroup, WithinLogBounds(offsets, shardLog.NextOffsetToApply), cancellationToken);
        } else {
            await replica.ProposeAsync(new AckRequest(consumerGroup, offsets), cancellationToken);
        }

        RecordAckMetrics(streamName, replica.GroupId, consumerGroup, offsets, shardLog);
    }

    /// <summary>
    /// Nacks the message at <paramref name="offset"/> for <paramref name="consumerGroup"/>: proposes a
    /// retry after <paramref name="delay"/> or the group's backoff, or parks the message once the
    /// group's retries are used up. Completes once this node's ack index applied the nack, so a
    /// consume stream here offers the nacked message again before any later one. Does nothing for an
    /// offset that is already acked or parked, or that the log doesn't hold.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <param name="offset">The nacked offset.</param>
    /// <param name="reason">Why handling failed; <see langword="null"/> for none.</param>
    /// <param name="delay">The delay before the next attempt; <see langword="null"/> for the group's backoff.</param>
    /// <param name="replica">The replica of the group serving the virtual shard.</param>
    /// <param name="shardLog">That group's shard log.</param>
    /// <param name="ackIndex">That group's ack index.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns><see langword="true"/> when the message was parked.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delay"/> is negative.</exception>
    /// <exception cref="NotLeaderException"><paramref name="replica"/> doesn't lead its group.</exception>
    public async Task<bool> NackAsync(
        string streamName,
        string consumerGroup,
        ulong offset,
        string? reason,
        TimeSpan? delay,
        RaftReplica replica,
        ShardLog shardLog,
        AckIndex ackIndex,
        CancellationToken cancellationToken
    ) {
        if (delay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "A nack's delay can't be negative.");

        var state = ackIndex.GetOrCreate(consumerGroup);
        if (offset >= shardLog.NextOffsetToApply || state.IsAcked(offset))
            return false;

        // The attempt that failed: the one the pending retry handed out, or else the first delivery.
        var attempt = state.Retry is { } retry && retry.Offset == offset ? retry.Attempt : 1;
        var policy = assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup);
        var park = attempt > policy.MaxRetries;
        var keptReason = reason is null ? "" : reason.Length <= MaxReasonLength ? reason : reason[..MaxReasonLength];
        var result = await replica.ProposeAsync(
            new NackRequest(consumerGroup, offset, attempt, park ? TimeSpan.Zero : delay ?? RetryDelay(policy, attempt), park, keptReason), cancellationToken);

        // Committed either way; past the timeout this node merely lags in applying it.
        await ConditionPoller.WaitUntilAsync(() => ackIndex.AppliedRaftIndex >= result.RaftIndex, NackApplyTimeout, cancellationToken: cancellationToken);

        if (park) {
            metrics.RecordParked(streamName, replica.GroupId, consumerGroup);
            logger.LogWarning("Parked offset {Offset} of stream {StreamName} for consumer group {ConsumerGroup} after {Attempts} attempt(s): {Reason}",
                offset, streamName, consumerGroup, attempt, keptReason);
        } else {
            metrics.RecordRetried(streamName, replica.GroupId, consumerGroup);
        }

        return park;
    }

    /// <summary>
    /// The group's backoff before the retry that follows the failed <paramref name="attempt"/>:
    /// <see cref="ConsumerGroupPolicy.InitialRetryDelay"/>, doubled for every earlier attempt, at
    /// most <see cref="ConsumerGroupPolicy.MaxRetryDelay"/>.
    /// </summary>
    /// <param name="policy">The group's policy.</param>
    /// <param name="attempt">The attempt that failed, 1 for the first.</param>
    /// <returns>The delay.</returns>
    internal static TimeSpan RetryDelay(ConsumerGroupPolicy policy, int attempt) =>
        // In doubles: doubling the ticks themselves overflows a long for a large attempt, long before the cap applies.
        TimeSpan.FromTicks((long)Math.Min(policy.InitialRetryDelay.Ticks * Math.Pow(2, attempt - 1), policy.MaxRetryDelay.Ticks));

    /// <summary>
    /// Whether <paramref name="consumerGroup"/> must consume from its group's leader.
    /// </summary>
    /// <remarks>
    /// Follower reads are safe by construction (State Machine Safety: whatever a follower has
    /// already applied is identical to what the leader applied), just possibly a fraction behind
    /// the leader. Strict mode's own guarantee is deliberately not to let any such lag stack on top
    /// of what it already promises, so it stays pinned; checkpoint mode already accepts a looser
    /// guarantee elsewhere (an unreplicated ack window), so this same follower-lag is consistent
    /// with it, not a new relaxation.
    /// </remarks>
    /// <param name="streamName">The stream.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <returns><see langword="true"/> in strict mode.</returns>
    public bool RequiresLeaderPinning(string streamName, string consumerGroup) =>
        assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup).Mode == AckMode.Strict;

    /// <summary>
    /// Streams every message from <paramref name="consumerGroup"/>'s first not-yet-acked offset on, in
    /// offset order, live-tailing as new messages arrive, each with its delivery attempt. Skips what
    /// the group acked or parked meanwhile. A nacked message is offered again once its retry is due,
    /// before any later one: the stream goes back to it, also when it already handed out later
    /// messages. The caller disposes each handle.
    /// </summary>
    /// <param name="shardLog">The group's shard log.</param>
    /// <param name="ackIndex">The group's ack index.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <param name="cancellationToken">Ends the stream.</param>
    /// <returns>The messages with their attempt, in offset order; never ends on its own.</returns>
    public static async IAsyncEnumerable<(LogEntryHandle Handle, int Attempt)> ConsumeAsync(
        ShardLog shardLog,
        AckIndex ackIndex,
        string consumerGroup,
        [EnumeratorCancellation] CancellationToken cancellationToken
    ) {
        // ShardLog's own reads are point-in-time snapshots, so this alternates draining what's
        // currently available with waiting for the next Append or nack. Both waits are taken before
        // the read: an Append or nack applied after the snapshot but before the wait would otherwise
        // go unnoticed until the next one.
        var state = ackIndex.GetOrCreate(consumerGroup);
        var offset = state.LowWatermark; // exclusive — already the first not-yet-acked offset
        PendingRetry? handledRetry = null; // the retry this stream went back for or handed out: never again for the same one
        while (true) {
            var appended = shardLog.WaitForAppendAsync(cancellationToken);
            var retryChanged = state.RetryChanged;
            var reread = false;
            var pending = state.Retry;
            if (pending is not null && pending.Offset < offset && !ReferenceEquals(pending, handledRetry)) {
                // Nacked after this stream handed it out, with nothing later to read yet (the stream
                // sat at the head of the log): go back to it before waiting for anything new.
                offset = pending.Offset;
                handledRetry = pending;
            }

            await foreach (var handle in shardLog.ReadFromAsync(offset, cancellationToken)) {
                var entryOffset = handle.Entry.LogicalOffset;
                var retry = state.Retry;
                if (retry is not null && retry.Offset < entryOffset && !ReferenceEquals(retry, handledRetry)) {
                    // Nacked after this stream handed it out: go back to it before anything later.
                    // Once per retry: a message pruned past its maximum retention never comes back.
                    handle.Dispose();
                    offset = retry.Offset;
                    handledRetry = retry;
                    reread = true;
                    break;
                }

                if (state.IsAcked(entryOffset)) {
                    handle.Dispose();
                    offset = entryOffset + 1;
                    continue;
                }

                var attempt = 1;
                if (retry is not null && retry.Offset == entryOffset) {
                    var wait = DateTimeOffset.FromUnixTimeMilliseconds((long)(retry.NotBeforeMicros / 1000)) - DateTimeOffset.UtcNow;
                    if (wait > TimeSpan.Zero) {
                        // Not due yet: hand out nothing until it is, or until the retry changes.
                        handle.Dispose();
                        await Task.WhenAny(Task.Delay(wait, cancellationToken), retryChanged);
                        offset = entryOffset;
                        reread = true;
                        break;
                    }

                    attempt = retry.Attempt;
                    handledRetry = retry; // a stream that reached it without going back must not go back for it later
                }

                offset = entryOffset + 1;
                yield return (handle, attempt);
            }

            if (!reread)
                await Task.WhenAny(appended, retryChanged);
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    /// <summary>
    /// Starts the <c>receive</c> span of one delivered message: a trace of its own, without a parent,
    /// linked to the publish its <c>traceparent</c> header names. A link, never a parent: a trace
    /// that stayed open from a publish until a possibly much later, possibly repeated receive
    /// wouldn't match how tracing backends expect a trace to behave, and every consumer group
    /// receives the same message at its own time. Without a parent, too: a consume stream stays open
    /// for hours, and thousands of children under its span would make one unusable trace.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="consumerGroup">The consumer group receiving the message.</param>
    /// <param name="headers">The message's stored headers.</param>
    /// <returns>The started span, to dispose once the message is handed over; <see langword="null"/> when nobody listens.</returns>
    public static Activity? StartReceiveActivity(string streamName, string consumerGroup, IReadOnlyList<Header> headers) {
        if (!RemontoireActivitySource.Source.HasListeners())
            return null;

        ActivityLink[]? links = null;
        foreach (var header in headers) {
            if (!HeaderKeyEquals(header, TraceParentHeaderKey))
                continue;

            if (ActivityContext.TryParse(Encoding.UTF8.GetString(header.Value.Span), traceState: null, out var publishContext))
                links = [new ActivityLink(publishContext)];
            break;
        }

        return RemontoireActivitySource.StartActivity($"receive {streamName}", parentContext: null, ActivityKind.Consumer, links)
            ?.SetTag("messaging.system", "remontoire")
            .SetTag("messaging.operation.type", "receive")
            .SetTag("messaging.operation.name", "receive")
            .SetTag("messaging.destination.name", streamName)
            .SetTag("messaging.consumer.group.name", consumerGroup);
    }

    /// <summary>
    /// Checkpoint-mode Ack's only defense against a client forging offsets far ahead of what was
    /// actually delivered: keeps only the offsets below <paramref name="nextOffsetToApply"/>.
    /// </summary>
    /// <param name="offsets">The offsets a client acknowledged.</param>
    /// <param name="nextOffsetToApply">One past the highest offset the log holds.</param>
    /// <returns>The offsets that refer to a message that exists.</returns>
    internal static IReadOnlyList<ulong> WithinLogBounds(IReadOnlyList<ulong> offsets, ulong nextOffsetToApply) =>
        offsets.Where(offset => offset < nextOffsetToApply).ToArray();

    static bool HeaderKeyEquals(Header header, string key) =>
        Encoding.UTF8.GetByteCount(key) == header.Key.Length && Encoding.UTF8.GetString(header.Key.Span) == key;

    // The acked counter carries consumer_group: a plain counter is cheap regardless of
    // cardinality. The latency needs one ShardLog lookup per acked offset to read back the
    // message's ingest timestamp, and is never consumer_group-tagged: a histogram keeps
    // per-tag-combination state for the process's whole lifetime, and consumer groups are the
    // least bounded dimension available here.
    void RecordAckMetrics(string streamName, string groupId, string consumerGroup, IReadOnlyList<ulong> offsets, ShardLog shardLog) {
        metrics.RecordAcked(streamName, groupId, consumerGroup, offsets.Count);

        var nowMicros = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
        foreach (var offset in offsets) {
            if (!shardLog.TryGet(offset, out var handle))
                continue;

            using (handle)
                // Both operands cast to long before subtracting, not after: TimestampMicros was
                // stamped by whichever node was leader at ingest time, nowMicros by whichever node
                // handles this Ack (possibly a different one, via a follower read) — clock skew
                // between them can make TimestampMicros briefly exceed nowMicros, which a plain
                // ulong subtraction would silently wrap into a multi-billion-second latency instead
                // of clamping to zero.
                metrics.RecordAckLatency(streamName, groupId, Math.Max(0L, (long)nowMicros - (long)handle.Entry.TimestampMicros) / 1_000_000.0);
        }
    }
}
