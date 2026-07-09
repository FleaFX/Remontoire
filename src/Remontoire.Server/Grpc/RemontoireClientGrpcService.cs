using System.Runtime.CompilerServices;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Remontoire.Client.V1;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Storage;

namespace Remontoire.Server.Grpc;

/// <summary>
/// The client-facing RPCs (<c>Publish</c>/<c>Consume</c>/<c>Ack</c>) — thin, exactly like <see
/// cref="RaftTransportGrpcService"/>: resolves <c>stream_name</c> via the registries and dispatches
/// straight to <see cref="RaftReplica.ProposeAsync(AppendRequest, CancellationToken)"/>'s two
/// overloads or to <see cref="ShardLog"/>/<see cref="AckIndex"/> directly. Never touches the
/// filesystem or segments itself.
/// </summary>
public sealed class RemontoireClientGrpcService(
    RaftReplicaRegistry raftRegistry, MessagingGroupRegistry messagingRegistry, LeaderAddressDirectory leaderAddresses)
    : RemontoireClient.RemontoireClientBase {
    /// <inheritdoc />
    public override async Task<PublishReply> Publish(PublishRequest request, ServerCallContext context) {
        var (replica, _, _) = Resolve(request.StreamName);

        var appendRequest = new AppendRequest(
            request.PartitionKey.Memory,
            request.Headers.Select(header => new Header(header.Key.Memory, header.Value.Memory)).ToArray(),
            request.Payload.Memory);

        try {
            var result = await replica.ProposeAsync(appendRequest, context.CancellationToken);
            return new PublishReply {
                Success = new PublishSuccess {
                    ShardId = 0, // exactly one virtual shard for now — no sharding/multi-group yet
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
        var (replica, _, _) = Resolve(request.StreamName);

        try {
            await replica.ProposeAsync(new Remontoire.Raft.AckRequest(request.ConsumerGroup, request.Offsets), context.CancellationToken);
            return new AckReply { Success = new Empty() };
        } catch (NotLeaderException ex) {
            return new AckReply { NotLeader = BuildNotLeader(request.StreamName, ex) };
        }
    }

    /// <inheritdoc />
    public override async Task Consume(ConsumeRequest request, IServerStreamWriter<ConsumeReply> responseStream, ServerCallContext context) {
        var (replica, shardLog, ackIndex) = Resolve(request.StreamName);

        // Every consumer group is pinned to the leader for now — strict-ack mode always is, and
        // no other mode exists yet, so the policy question is trivially answered until it does.
        if (RequiresLeaderPinning(request.ConsumerGroup) && !replica.IsLeader) {
            await responseStream.WriteAsync(
                new ConsumeReply { NotLeader = BuildNotLeader(request.StreamName, new NotLeaderException(replica.GroupId, replica.LeaderHint)) });
            return;
        }

        var watermark = ackIndex.GetOrCreate(request.ConsumerGroup).LowWatermark; // exclusive — already the first not-yet-acked offset
        await foreach (var handle in ReadLiveAsync(shardLog, watermark, context.CancellationToken))
            using (handle)
                await responseStream.WriteAsync(new ConsumeReply { Message = ToProto(handle.Entry) });
    }

    static bool RequiresLeaderPinning(string consumerGroup) => true;

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

    // stream_name and group_id are identical in this simplified, single-group configuration — no
    // stored virtual-shard/physical-group mapping exists yet (that needs a later phase's control
    // plane). Resolving straight through the two registries is therefore already correct, not a
    // stand-in for a lookup that's missing.
    (RaftReplica Replica, ShardLog ShardLog, AckIndex AckIndex) Resolve(string streamName) {
        if (!raftRegistry.TryGet(streamName, out var replica) || !messagingRegistry.TryGet(streamName, out var messaging))
            throw new RpcException(new Status(StatusCode.NotFound, $"No stream '{streamName}' is hosted here."));

        return (replica, messaging.ShardLog, messaging.AckIndex);
    }
}
