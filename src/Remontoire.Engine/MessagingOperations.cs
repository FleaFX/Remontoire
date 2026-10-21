using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Remontoire.Messaging;
using Remontoire.Observability;
using Remontoire.Raft;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

/// <summary>
/// Publish, ack and consume against a group this process hosts — the one implementation behind the
/// gRPC client endpoint and the embedded producer and consumer, so both behave the same. Resolving
/// the group, and answering when it isn't hosted here, stays with the caller.
/// </summary>
/// <param name="assignmentTable">The source of every stream's consumer-group policies.</param>
public sealed class MessagingOperations(ShardAssignmentTable assignmentTable) {
    /// <summary>
    /// The header key a message's own correlation id rides under — durably stored alongside the
    /// message (the existing free-form headers mechanism), so a later Consume/Ack of that same
    /// message can recover the same value. Unified with OpenTelemetry's own trace id rather than
    /// inventing a second, parallel id scheme.
    /// </summary>
    public const string CorrelationIdHeaderKey = "correlation-id";

    /// <summary>
    /// Proposes one message to <paramref name="replica"/> and completes once it is committed.
    /// Without a caller-supplied correlation id, the current trace id is stored as one.
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
        var storedHeaders = headers.ToList();
        // If the caller didn't already supply its own correlation-id, the current trace-id doubles
        // as one for this message — every later Consume/Ack of it can then link back to this same
        // Publish trace, even though tracing itself never forces one unbroken trace across both.
        if (Activity.Current?.Id is { } currentTraceparent && !storedHeaders.Any(header => HeaderKeyEquals(header, CorrelationIdHeaderKey)))
            storedHeaders.Add(new Header(Encoding.UTF8.GetBytes(CorrelationIdHeaderKey), Encoding.UTF8.GetBytes(currentTraceparent)));

        var result = await replica.ProposeAsync(new AppendRequest(partitionKey, storedHeaders, payload), cancellationToken);
        RemontoireMetrics.IngestMessagesTotal.Add(1,
            new KeyValuePair<string, object?>("stream", streamName),
            new KeyValuePair<string, object?>("shard", replica.GroupId));
        return result;
    }

    /// <summary>
    /// Acknowledges <paramref name="offsets"/> for <paramref name="consumerGroup"/> the way the
    /// group's ack mode asks: through Raft in strict mode, locally on the leader in checkpoint mode.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="shardId">The virtual shard, for the metrics.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <param name="offsets">The acknowledged offsets.</param>
    /// <param name="replica">The replica of the group serving the virtual shard.</param>
    /// <param name="shardLog">That group's shard log.</param>
    /// <param name="ackIndex">That group's ack index.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <exception cref="NotLeaderException"><paramref name="replica"/> doesn't lead its group.</exception>
    public async Task AckAsync(
        string streamName,
        int shardId,
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

        RecordAckMetrics(streamName, shardId, consumerGroup, offsets, shardLog);
    }

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
    /// Streams every message from <paramref name="consumerGroup"/>'s first not-yet-acked offset on,
    /// live-tailing as new messages arrive. The caller disposes each handle.
    /// </summary>
    /// <param name="shardLog">The group's shard log.</param>
    /// <param name="ackIndex">The group's ack index.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <param name="cancellationToken">Ends the stream.</param>
    /// <returns>The messages, in offset order; never ends on its own.</returns>
    public static async IAsyncEnumerable<LogEntryHandle> ConsumeAsync(
        ShardLog shardLog,
        AckIndex ackIndex,
        string consumerGroup,
        [EnumeratorCancellation] CancellationToken cancellationToken
    ) {
        // ShardLog's own reads are point-in-time snapshots, so this alternates draining what's
        // currently available with waiting for the next Append.
        var offset = ackIndex.GetOrCreate(consumerGroup).LowWatermark; // exclusive — already the first not-yet-acked offset
        while (true) {
            await foreach (var handle in shardLog.ReadFromAsync(offset, cancellationToken)) {
                offset = handle.Entry.LogicalOffset + 1;
                yield return handle;
            }

            await shardLog.WaitForAppendAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Links the current activity to the trace a message's stored correlation id names — a Link,
    /// never a parent: a trace that stayed open from Publish until a possibly-much-later,
    /// possibly-repeated, possibly-never-arriving Consume wouldn't match how tracing backends expect
    /// a trace's lifetime to behave, and a Link is the only shape that survives independent consumer
    /// groups each reading the same message at their own, unrelated time.
    /// </summary>
    /// <param name="headers">The message's stored headers.</param>
    public static void LinkToStoredCorrelationContext(IReadOnlyList<Header> headers) {
        if (Activity.Current is not { } activity)
            return;

        foreach (var header in headers) {
            if (!HeaderKeyEquals(header, CorrelationIdHeaderKey))
                continue;

            var traceparent = Encoding.UTF8.GetString(header.Value.Span);
            if (ActivityContext.TryParse(traceparent, traceState: null, out var linkedContext))
                activity.AddLink(new ActivityLink(linkedContext));

            return;
        }
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

    // remontoire_ack_messages_total: no consumer_group dimension risk here — a plain counter is
    // cheap regardless of cardinality. remontoire_ack_latency_seconds needs one ShardLog lookup per
    // acked offset to read back the message's own ingest timestamp — never consumer_group-tagged,
    // since a histogram (unlike a counter) keeps per-tag-combination state for the process's whole
    // lifetime, and consumer groups are the least bounded dimension available here.
    static void RecordAckMetrics(string streamName, int shardId, string consumerGroup, IReadOnlyList<ulong> offsets, ShardLog shardLog) {
        RemontoireMetrics.AckMessagesTotal.Add(offsets.Count,
            new KeyValuePair<string, object?>("stream", streamName),
            new KeyValuePair<string, object?>("shard", shardId.ToString()),
            new KeyValuePair<string, object?>("consumer_group", consumerGroup));

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
                RemontoireMetrics.AckLatencySeconds.Record(Math.Max(0L, (long)nowMicros - (long)handle.Entry.TimestampMicros) / 1_000_000.0,
                    new KeyValuePair<string, object?>("stream", streamName),
                    new KeyValuePair<string, object?>("shard", shardId.ToString()));
        }
    }
}
