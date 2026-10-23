using System.Runtime.CompilerServices;
using System.Text;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

/// <summary>
/// The in-process producer and consumer of the embedded engine: runs the same
/// <see cref="MessagingOperations"/> the gRPC client endpoint runs, against the groups this process
/// hosts.
/// </summary>
sealed class EmbeddedRemontoire(
    RaftReplicaRegistry raftRegistry,
    MessagingGroupRegistry messagingRegistry,
    ShardAssignmentTable assignmentTable,
    MessagingOperations operations
) : IRemontoireProducer, IRemontoireConsumer {

    /// <inheritdoc />
    public async Task<PublishResult> PublishAsync(
        string streamName,
        string partitionKey,
        ReadOnlyMemory<byte> payload,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default
    ) {
        var config = StreamConfig(streamName);
        var key = Encoding.UTF8.GetBytes(partitionKey);
        var virtualShardIndex = ShardRouter.GetVirtualShardIndex(key, config.VirtualShardCount, config.RoutingAlgorithm);
        var (replica, _, _) = Resolve(streamName, virtualShardIndex);

        IEnumerable<Header> storedHeaders = headers?.Select(header => new Header(Encoding.UTF8.GetBytes(header.Key), Encoding.UTF8.GetBytes(header.Value))) ?? [];
        var result = await operations.PublishAsync(streamName, replica, key, storedHeaders, payload, cancellationToken);
        return new PublishResult(virtualShardIndex, (long)result.LogicalOffset, ToDateTimeOffset(result.TimestampMicros));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RemontoireMessage> ConsumeAsync(
        string streamName,
        string consumerGroup,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    ) {
        StreamConfig(streamName);
        RequireConsumerGroup(streamName, consumerGroup);
        var (_, shardLog, ackIndex) = Resolve(streamName, virtualShardIndex: 0);
        await foreach (var (handle, attempt) in MessagingOperations.ConsumeAsync(shardLog, ackIndex, consumerGroup, cancellationToken)) {
            RemontoireMessage message;
            using (handle) {
                using var receive = MessagingOperations.StartReceiveActivity(streamName, consumerGroup, handle.Entry.Headers);
                message = ToMessage(handle.Entry, attempt);
            }

            yield return message;
        }
    }

    /// <inheritdoc />
    public async Task AckAsync(string streamName, string consumerGroup, int shardId, long offset, CancellationToken cancellationToken = default) {
        StreamConfig(streamName);
        RequireConsumerGroup(streamName, consumerGroup);
        var (replica, shardLog, ackIndex) = Resolve(streamName, virtualShardIndex: 0);
        await operations.AckAsync(streamName, consumerGroup, [(ulong)offset], replica, shardLog, ackIndex, cancellationToken);
    }

    /// <inheritdoc />
    public async Task NackAsync(
        string streamName,
        string consumerGroup,
        int shardId,
        long offset,
        string? reason = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default
    ) {
        StreamConfig(streamName);
        RequireConsumerGroup(streamName, consumerGroup);
        var (replica, shardLog, ackIndex) = Resolve(streamName, virtualShardIndex: 0);
        await operations.NackAsync(streamName, consumerGroup, (ulong)offset, reason, delay, replica, shardLog, ackIndex, cancellationToken);
    }

    StreamShardingConfig StreamConfig(string streamName) =>
        assignmentTable.TryGetStreamConfig(streamName, out var config)
            ? config
            : throw new ArgumentException($"No stream '{streamName}' exists.", nameof(streamName));

    void RequireConsumerGroup(string streamName, string consumerGroup) {
        if (!assignmentTable.TryGetConsumerGroupPolicy(streamName, consumerGroup, out _))
            throw new ArgumentException($"No consumer group '{consumerGroup}' is declared on stream '{streamName}'.", nameof(consumerGroup));
    }

    (RaftReplica Replica, ShardLog ShardLog, AckIndex AckIndex) Resolve(string streamName, int virtualShardIndex) =>
        TryResolve(streamName, virtualShardIndex, out var shard)
            ? shard
            : throw new InvalidOperationException($"Stream '{streamName}' is not open yet.");

    bool TryResolve(string streamName, int virtualShardIndex, out (RaftReplica Replica, ShardLog ShardLog, AckIndex AckIndex) shard) {
        shard = default;
        if (!assignmentTable.TryGetAssignment(streamName, virtualShardIndex, out var assignment)
            || !raftRegistry.TryGet(assignment.GroupId, out var replica) || !replica.IsLeader
            || !messagingRegistry.TryGet(assignment.GroupId, out var messaging))
            return false;

        shard = (replica, messaging.ShardLog, messaging.AckIndex);
        return true;
    }

    static RemontoireMessage ToMessage(LogEntry entry, int attempt) {
        var headers = new Dictionary<string, string>();
        foreach (var header in entry.Headers)
            headers[Encoding.UTF8.GetString(header.Key.Span)] = Encoding.UTF8.GetString(header.Value.Span);

        return new RemontoireMessage(ShardId: 0, (long)entry.LogicalOffset, Encoding.UTF8.GetString(entry.PartitionKey.Span),
            entry.Payload.ToArray(), headers, ToDateTimeOffset(entry.TimestampMicros), attempt);
    }

    // The same conversion the gRPC client uses.
    static DateTimeOffset ToDateTimeOffset(ulong micros) => DateTimeOffset.UnixEpoch.AddTicks((long)micros * 10);
}
