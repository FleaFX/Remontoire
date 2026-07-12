using System.Runtime.CompilerServices;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Remontoire.Client.V1;
using Remontoire.Messaging;
using Remontoire.Observability;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server.Grpc;

/// <summary>
/// The client-facing RPCs (<c>Publish</c>/<c>Consume</c>/<c>Ack</c>) — thin, exactly like <see
/// cref="RaftTransportGrpcService"/>: resolves <c>stream_name</c> (and, for <c>Publish</c>, a
/// partition key) to a virtual shard and its current physical group via a <see cref="ShardAssignmentTable"/>,
/// then dispatches straight to <see cref="RaftReplica.ProposeAsync(AppendRequest, CancellationToken)"/>'s
/// two overloads or to <see cref="ShardLog"/>/<see cref="AckIndex"/> directly. Never touches the
/// filesystem or segments itself.
/// </summary>
/// <remarks>
/// <c>Consume</c>/<c>Ack</c> carry no partition key on the wire yet — routing multiple virtual
/// shards' worth of consumption is still an open, unresolved wire-contract question, so both
/// always resolve virtual shard 0 for now, same as every RPC did before this class knew about
/// sharding at all.
/// </remarks>
public sealed class RemontoireClientGrpcService(
    RaftReplicaRegistry raftRegistry, MessagingGroupRegistry messagingRegistry, LeaderAddressDirectory leaderAddresses,
    ShardAssignmentTable assignmentTable, MigrationAdmissionGate admissionGate)
    : RemontoireClient.RemontoireClientBase {
    /// <inheritdoc />
    public override async Task<PublishReply> Publish(PublishRequest request, ServerCallContext context) {
        if (!assignmentTable.TryGetStreamConfig(request.StreamName, out var config))
            throw new RpcException(new Status(StatusCode.NotFound, $"No stream '{request.StreamName}' is hosted here."));

        var virtualShardIndex = ShardRouter.GetVirtualShardIndex(request.PartitionKey.Span, config.VirtualShardCount, config.RoutingAlgorithm);
        var redirect = TryResolveGroup(request.StreamName, virtualShardIndex, out var replica, out _, out _);
        if (redirect is not null)
            return new PublishReply { NotLeader = redirect };

        if (admissionGate.IsPaused(replica.GroupId))
            return new PublishReply { ShardMigrating = new ShardMigrating { StreamName = request.StreamName } };

        var appendRequest = new AppendRequest(
            request.PartitionKey.Memory,
            request.Headers.Select(header => new Header(header.Key.Memory, header.Value.Memory)).ToArray(),
            request.Payload.Memory);

        try {
            var result = await replica.ProposeAsync(appendRequest, context.CancellationToken);
            RemontoireMetrics.IngestMessagesTotal.Add(1,
                new KeyValuePair<string, object?>("stream", request.StreamName),
                new KeyValuePair<string, object?>("shard", replica.GroupId));
            return new PublishReply {
                Success = new PublishSuccess {
                    ShardId = virtualShardIndex,
                    Offset = result.LogicalOffset,
                    IngestedAtMicros = result.TimestampMicros,
                },
            };
        } catch (NotLeaderException ex) {
            return new PublishReply { NotLeader = BuildNotLeader(request.StreamName, ex) };
        }
    }

    /// <inheritdoc />
    public override async Task<AckReply> Ack(Remontoire.Client.V1.AckRequest request, ServerCallContext context) {
        if (!assignmentTable.TryGetStreamConfig(request.StreamName, out _))
            throw new RpcException(new Status(StatusCode.NotFound, $"No stream '{request.StreamName}' is hosted here."));

        var redirect = TryResolveGroup(request.StreamName, virtualShardIndex: 0, out var replica, out var shardLog, out var ackIndex);
        if (redirect is not null)
            return new AckReply { NotLeader = redirect };

        if (admissionGate.IsPaused(replica.GroupId))
            return new AckReply { ShardMigrating = new ShardMigrating { StreamName = request.StreamName } };

        var mode = assignmentTable.GetConsumerGroupPolicy(request.StreamName, request.ConsumerGroup).Mode;

        if (mode == AckMode.Checkpoint) {
            // Today's leader-pinning for Ack is an accidental side effect of always calling
            // ProposeAsync (which throws NotLeaderException off a non-ready-leader replica). That
            // side effect disappears the moment this path skips ProposeAsync entirely — this check
            // is now load-bearing, not incidental: a checkpoint-mode ack applied on a follower would
            // silently vanish at that follower's own next crash, before the leader's own periodic
            // checkpoint (AckCheckpointer) ever captures it.
            if (!replica.IsLeader)
                return new AckReply { NotLeader = BuildNotLeader(request.StreamName, new NotLeaderException(replica.GroupId, replica.LeaderHint)) };

            // Checkpoint mode skips Raft entirely (that's the whole point), so nothing else ever
            // validates these client-supplied offsets — unlike the strict path below, forging one
            // here costs no round-trip. Silently drop anything referring to a message that doesn't
            // exist yet rather than letting it advance this group's watermark past undelivered data.
            await ackIndex.ApplyLocalAsync(request.ConsumerGroup, WithinLogBounds(request.Offsets, shardLog.NextOffsetToApply), context.CancellationToken);
            RecordAckMetrics(request, shardLog);
            return new AckReply { Success = new Empty() };
        }

        try {
            await replica.ProposeAsync(new Remontoire.Raft.AckRequest(request.ConsumerGroup, request.Offsets), context.CancellationToken);
            RecordAckMetrics(request, shardLog);
            return new AckReply { Success = new Empty() };
        } catch (NotLeaderException ex) {
            return new AckReply { NotLeader = BuildNotLeader(request.StreamName, ex) };
        }
    }

    // remontoire_ack_messages_total: no consumer_group dimension risk here — a plain counter is
    // cheap regardless of cardinality. remontoire_ack_latency_seconds needs one ShardLog lookup per
    // acked offset to read back the message's own ingest timestamp — never consumer_group-tagged,
    // since a histogram (unlike a counter) keeps per-tag-combination state for the process's whole
    // lifetime, and consumer groups are the least bounded dimension available here.
    static void RecordAckMetrics(Remontoire.Client.V1.AckRequest request, ShardLog shardLog) {
        RemontoireMetrics.AckMessagesTotal.Add(request.Offsets.Count,
            new KeyValuePair<string, object?>("stream", request.StreamName),
            new KeyValuePair<string, object?>("shard", request.ShardId.ToString()),
            new KeyValuePair<string, object?>("consumer_group", request.ConsumerGroup));

        var nowMicros = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
        foreach (var offset in request.Offsets) {
            if (!shardLog.TryGet(offset, out var handle))
                continue;

            using (handle)
                RemontoireMetrics.AckLatencySeconds.Record((nowMicros - handle.Entry.TimestampMicros) / 1_000_000.0,
                    new KeyValuePair<string, object?>("stream", request.StreamName),
                    new KeyValuePair<string, object?>("shard", request.ShardId.ToString()));
        }
    }

    /// <inheritdoc />
    public override async Task Consume(ConsumeRequest request, IServerStreamWriter<ConsumeReply> responseStream, ServerCallContext context) {
        if (!assignmentTable.TryGetStreamConfig(request.StreamName, out _))
            throw new RpcException(new Status(StatusCode.NotFound, $"No stream '{request.StreamName}' is hosted here."));

        var redirect = TryResolveGroup(request.StreamName, virtualShardIndex: 0, out var replica, out var shardLog, out var ackIndex);
        if (redirect is not null) {
            await responseStream.WriteAsync(new ConsumeReply { NotLeader = redirect });
            return;
        }

        if (admissionGate.IsPaused(replica.GroupId)) {
            await responseStream.WriteAsync(new ConsumeReply { ShardMigrating = new ShardMigrating { StreamName = request.StreamName } });
            return;
        }

        if (RequiresLeaderPinning(request.StreamName, request.ConsumerGroup) && !replica.IsLeader) {
            await responseStream.WriteAsync(
                new ConsumeReply { NotLeader = BuildNotLeader(request.StreamName, new NotLeaderException(replica.GroupId, replica.LeaderHint)) });
            return;
        }

        var watermark = ackIndex.GetOrCreate(request.ConsumerGroup).LowWatermark; // exclusive — already the first not-yet-acked offset
        await foreach (var handle in ReadLiveAsync(shardLog, watermark, context.CancellationToken))
            using (handle)
                await responseStream.WriteAsync(new ConsumeReply { Message = ToProto(handle.Entry) });
    }

    // Follower reads are safe by construction (State Machine Safety: whatever a follower has
    // already applied is identical to what the leader applied), just possibly a fraction behind
    // the leader. Strict mode's own guarantee is deliberately not to let any such lag stack on top
    // of what it already promises, so it stays pinned; checkpoint mode already accepts a looser
    // guarantee elsewhere (an unreplicated ack window), so this same follower-lag is consistent
    // with it, not a new relaxation.
    bool RequiresLeaderPinning(string streamName, string consumerGroup) =>
        assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup).Mode == AckMode.Strict;

    // Streaming's own long-lived "keep reading as new data arrives" loop — ShardLog's own reads
    // are point-in-time snapshots, so this alternates draining what's currently available with
    // waiting for the next Append.
    static async IAsyncEnumerable<LogEntryHandle> ReadLiveAsync(ShardLog shardLog, ulong fromOffset, [EnumeratorCancellation] CancellationToken cancellationToken) {
        var offset = fromOffset;
        while (true) {
            await foreach (var handle in shardLog.ReadFromAsync(offset, cancellationToken)) {
                offset = handle.Entry.LogicalOffset + 1;
                yield return handle;
            }

            await shardLog.WaitForAppendAsync(cancellationToken);
        }
    }

    static RemontoireMessageProto ToProto(LogEntry entry) {
        var proto = new RemontoireMessageProto {
            ShardId = 0,
            Offset = entry.LogicalOffset,
            PartitionKey = ByteString.CopyFrom(entry.PartitionKey.Span),
            Payload = ByteString.CopyFrom(entry.Payload.Span),
            IngestedAtMicros = entry.TimestampMicros,
        };
        proto.Headers.Add(entry.Headers.Select(header => new MessageHeader {
            Key = ByteString.CopyFrom(header.Key.Span), Value = ByteString.CopyFrom(header.Value.Span),
        }));
        return proto;
    }

    // The redirect target's resolved address, not a bare node ID — the client cannot do this
    // translation itself (RaftReplica.LeaderHint is a node ID, the right concept inside the group
    // itself, not a network address). Null stays null: "no hint, an election is in progress."
    NotLeader BuildNotLeader(string streamName, NotLeaderException ex) =>
        new() { StreamName = streamName, LeaderAddress = ex.LeaderHint is { } nodeId ? leaderAddresses.TryGet(nodeId)?.ToString() : null };

    // Checkpoint-mode Ack's only defense against a client forging offsets far ahead of what was
    // actually delivered — extracted as its own static method so the filtering rule is directly
    // testable, independent of ServerCallContext (which every RPC method on this class needs, and
    // which has no fake precedent in this codebase).
    internal static IReadOnlyList<ulong> WithinLogBounds(IReadOnlyList<ulong> offsets, ulong nextOffsetToApply) =>
        offsets.Where(offset => offset < nextOffsetToApply).ToArray();

    // Resolves a known stream's virtual shard to its current physical group, then either
    // dispatches locally (the common case: this group is hosted right here) or builds a redirect
    // shaped exactly like an ordinary NotLeader reply — the client's existing retry loop doesn't
    // need to know whether the reason was "wrong leader within the right group" or "wrong group
    // entirely," both look identical to it: follow the hint, try again.
    NotLeader? TryResolveGroup(string streamName, int virtualShardIndex, out RaftReplica replica, out ShardLog shardLog, out AckIndex ackIndex) {
        if (!assignmentTable.TryGetAssignment(streamName, virtualShardIndex, out var assignment))
            throw new RpcException(new Status(StatusCode.Unavailable, $"No group assignment yet for '{streamName}' virtual shard {virtualShardIndex}."));

        if (raftRegistry.TryGet(assignment.GroupId, out replica) && messagingRegistry.TryGet(assignment.GroupId, out var messaging)) {
            shardLog = messaging.ShardLog;
            ackIndex = messaging.AckIndex;
            return null;
        }

        replica = null!;
        shardLog = null!;
        ackIndex = null!;
        return BuildGroupRedirect(streamName, assignment.GroupId);
    }

    NotLeader BuildGroupRedirect(string streamName, string groupId) {
        Uri? address = null;
        if (assignmentTable.TryGetGroup(groupId, out var group))
            foreach (var member in group.Members) {
                address = leaderAddresses.TryGet(member.NodeId);
                if (address is not null)
                    break;
            }

        return new NotLeader { StreamName = streamName, LeaderAddress = address?.ToString() };
    }
}
