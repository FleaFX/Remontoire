using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Remontoire.Client.V1;
using Remontoire.Engine;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// The client-facing RPCs (<c>Publish</c>/<c>Consume</c>/<c>Ack</c>/<c>Nack</c>) — resolves <c>stream_name</c>
/// (and, for <c>Publish</c>, a partition key) to a virtual shard and its current physical group via
/// a <see cref="ShardAssignmentTable"/>, answers with a redirect when that group isn't hosted here,
/// and otherwise leaves the work to <see cref="MessagingOperations"/>, the same operations the
/// embedded engine runs in-process. Never touches the filesystem or segments itself.
/// </summary>
/// <remarks>
/// <c>Consume</c>/<c>Ack</c> carry no partition key on the wire yet — routing multiple virtual
/// shards' worth of consumption is still an open, unresolved wire-contract question, so both
/// always resolve virtual shard 0 for now, same as every RPC did before this class knew about
/// sharding at all.
/// </remarks>
public sealed class RemontoireClientGrpcService(
    RaftReplicaRegistry raftRegistry, MessagingGroupRegistry messagingRegistry, LeaderAddressDirectory leaderAddresses,
    ShardAssignmentTable assignmentTable, MigrationAdmissionGate admissionGate, MessagingOperations operations)
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

        var headers = request.Headers.Select(header => new Header(header.Key.Memory, header.Value.Memory));
        try {
            var result = await operations.PublishAsync(request.StreamName, replica, request.PartitionKey.Memory, headers, request.Payload.Memory, context.CancellationToken);
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
        if (!assignmentTable.TryGetConsumerGroupPolicy(request.StreamName, request.ConsumerGroup, out _))
            throw new RpcException(new Status(StatusCode.NotFound,
                $"No consumer group '{request.ConsumerGroup}' is declared on stream '{request.StreamName}'."));

        var redirect = TryResolveGroup(request.StreamName, virtualShardIndex: 0, out var replica, out var shardLog, out var ackIndex);
        if (redirect is not null)
            return new AckReply { NotLeader = redirect };

        if (admissionGate.IsPaused(replica.GroupId))
            return new AckReply { ShardMigrating = new ShardMigrating { StreamName = request.StreamName } };

        try {
            await operations.AckAsync(request.StreamName, request.ConsumerGroup, request.Offsets, replica, shardLog, ackIndex, context.CancellationToken);
            return new AckReply { Success = new Empty() };
        } catch (NotLeaderException ex) {
            return new AckReply { NotLeader = BuildNotLeader(request.StreamName, ex) };
        }
    }

    /// <inheritdoc />
    public override async Task<NackReply> Nack(Remontoire.Client.V1.NackRequest request, ServerCallContext context) {
        if (!assignmentTable.TryGetStreamConfig(request.StreamName, out _))
            throw new RpcException(new Status(StatusCode.NotFound, $"No stream '{request.StreamName}' is hosted here."));
        if (!assignmentTable.TryGetConsumerGroupPolicy(request.StreamName, request.ConsumerGroup, out _))
            throw new RpcException(new Status(StatusCode.NotFound,
                $"No consumer group '{request.ConsumerGroup}' is declared on stream '{request.StreamName}'."));

        var delay = request.Delay?.ToTimeSpan();
        if (delay < TimeSpan.Zero)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "A nack's delay can't be negative."));

        var redirect = TryResolveGroup(request.StreamName, virtualShardIndex: 0, out var replica, out var shardLog, out var ackIndex);
        if (redirect is not null)
            return new NackReply { NotLeader = redirect };

        if (admissionGate.IsPaused(replica.GroupId))
            return new NackReply { ShardMigrating = new ShardMigrating { StreamName = request.StreamName } };

        try {
            var parked = await operations.NackAsync(request.StreamName, request.ConsumerGroup, request.Offset, request.Reason, delay,
                replica, shardLog, ackIndex, context.CancellationToken);
            return new NackReply { Success = new NackSuccess { Parked = parked } };
        } catch (NotLeaderException ex) {
            return new NackReply { NotLeader = BuildNotLeader(request.StreamName, ex) };
        }
    }

    /// <inheritdoc />
    public override async Task Consume(ConsumeRequest request, IServerStreamWriter<ConsumeReply> responseStream, ServerCallContext context) {
        if (!assignmentTable.TryGetStreamConfig(request.StreamName, out _))
            throw new RpcException(new Status(StatusCode.NotFound, $"No stream '{request.StreamName}' is hosted here."));
        if (!assignmentTable.TryGetConsumerGroupPolicy(request.StreamName, request.ConsumerGroup, out _))
            throw new RpcException(new Status(StatusCode.NotFound,
                $"No consumer group '{request.ConsumerGroup}' is declared on stream '{request.StreamName}'."));

        var redirect = TryResolveGroup(request.StreamName, virtualShardIndex: 0, out var replica, out var shardLog, out var ackIndex);
        if (redirect is not null) {
            await responseStream.WriteAsync(new ConsumeReply { NotLeader = redirect });
            return;
        }

        if (admissionGate.IsPaused(replica.GroupId)) {
            await responseStream.WriteAsync(new ConsumeReply { ShardMigrating = new ShardMigrating { StreamName = request.StreamName } });
            return;
        }

        if (operations.RequiresLeaderPinning(request.StreamName, request.ConsumerGroup) && !replica.IsLeader) {
            await responseStream.WriteAsync(
                new ConsumeReply { NotLeader = BuildNotLeader(request.StreamName, new NotLeaderException(replica.GroupId, replica.LeaderHint)) });
            return;
        }

        await foreach (var (handle, attempt) in MessagingOperations.ConsumeAsync(shardLog, ackIndex, request.ConsumerGroup, context.CancellationToken)) {
            using (handle) {
                using var receive = MessagingOperations.StartReceiveActivity(request.StreamName, request.ConsumerGroup, handle.Entry.Headers);
                await responseStream.WriteAsync(new ConsumeReply { Message = ToProto(handle.Entry, attempt) });
            }
        }
    }

    static RemontoireMessageProto ToProto(LogEntry entry, int attempt) {
        var proto = new RemontoireMessageProto {
            ShardId = 0,
            Offset = entry.LogicalOffset,
            PartitionKey = ByteString.CopyFrom(entry.PartitionKey.Span),
            Payload = ByteString.CopyFrom(entry.Payload.Span),
            IngestedAtMicros = entry.TimestampMicros,
            Attempt = (uint)attempt,
        };
        proto.Headers.Add(entry.Headers.Select(header => new MessageHeader {
            Key = ByteString.CopyFrom(header.Key.Span), Value = ByteString.CopyFrom(header.Value.Span),
        }));
        return proto;
    }

    // The redirect target's resolved address, not a bare node ID — the client cannot do this
    // translation itself (RaftReplica.LeaderHint is a node ID, the right concept inside the group
    // itself, not a network address). LeaderAddress stays unset (never assigned null directly):
    // "no hint, an election is in progress." An "optional string" field's generated setter still
    // rejects a literal null via ProtoPreconditions.CheckNotNull — only leaving the property
    // untouched keeps it correctly absent.
    NotLeader BuildNotLeader(string streamName, NotLeaderException ex) {
        var notLeader = new NotLeader { StreamName = streamName };
        if (ex.LeaderHint is { } nodeId && leaderAddresses.TryGet(nodeId) is { } address)
            notLeader.LeaderAddress = address.ToString();
        return notLeader;
    }

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
        var notLeader = new NotLeader { StreamName = streamName };
        if (assignmentTable.TryGetGroup(groupId, out var group))
            foreach (var member in group.Members) {
                if (leaderAddresses.TryGet(member.NodeId) is not { } address)
                    continue;
                notLeader.LeaderAddress = address.ToString();
                break;
            }

        return notLeader;
    }
}
