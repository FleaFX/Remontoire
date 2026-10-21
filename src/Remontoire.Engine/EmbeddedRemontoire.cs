using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

/// <summary>
/// The in-process producer, consumer and admin of the embedded engine: runs the same
/// <see cref="MessagingOperations"/> the gRPC client endpoint runs, against the groups this process
/// hosts.
/// </summary>
sealed partial class EmbeddedRemontoire(
    RemontoireEngineOptions options,
    RaftReplicaRegistry raftRegistry,
    MessagingGroupRegistry messagingRegistry,
    ShardAssignmentTable assignmentTable,
    MessagingOperations operations
) : IRemontoireProducer, IRemontoireConsumer, IRemontoireAdmin {
    static readonly TimeSpan CreateStreamTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Whether <paramref name="name"/> can name a stream, and with it the stream's group and its
    /// directory: 1 to 100 letters, digits, '.', '_' or '-', starting with a letter or digit.
    /// </summary>
    /// <param name="name">The candidate name.</param>
    /// <returns><see langword="true"/> for a valid name.</returns>
    internal static bool IsValidName(string name) => ValidName().IsMatch(name);

    [GeneratedRegex(@"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}\z")]
    private static partial Regex ValidName();

    /// <inheritdoc />
    public async Task CreateStreamAsync(string streamName, CancellationToken cancellationToken = default) {
        if (!IsValidName(streamName))
            throw new ArgumentException(
                $"'{streamName}' is not a valid stream name: use 1 to 100 letters, digits, '.', '_' or '-', starting with a letter or digit.",
                nameof(streamName));

        // One group per stream, named after it: its directory under groups/ carries the stream's name.
        if (!assignmentTable.TryGetAssignment(streamName, 0, out _)) {
            if (!raftRegistry.TryGet(RemontoireNode.MetaGroupId, out var metaReplica))
                throw new InvalidOperationException("Remontoire has not started.");

            var records = StreamBootstrap.Records(streamName, virtualShardCount: 1, RoutingAlgorithm.XxHash3V1,
                [new RegisterGroup(streamName, [EmbeddedHostedService.Member(options)])], [(0, streamName)]);
            foreach (var record in records)
                await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(record)), cancellationToken);
        }

        if (!await ConditionPoller.WaitUntilAsync(() => TryResolve(streamName, 0, out _), CreateStreamTimeout, cancellationToken: cancellationToken))
            throw new TimeoutException($"Stream '{streamName}' did not open within {CreateStreamTimeout.TotalSeconds} seconds; see the log.");
    }

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
        var (_, shardLog, ackIndex) = Resolve(streamName, virtualShardIndex: 0);
        await foreach (var handle in MessagingOperations.ConsumeAsync(shardLog, ackIndex, consumerGroup, cancellationToken)) {
            RemontoireMessage message;
            using (handle) {
                MessagingOperations.LinkToStoredCorrelationContext(handle.Entry.Headers);
                message = ToMessage(handle.Entry);
            }

            yield return message;
        }
    }

    /// <inheritdoc />
    public async Task AckAsync(string streamName, string consumerGroup, int shardId, long offset, CancellationToken cancellationToken = default) {
        StreamConfig(streamName);
        var (replica, shardLog, ackIndex) = Resolve(streamName, virtualShardIndex: 0);
        await operations.AckAsync(streamName, shardId, consumerGroup, [(ulong)offset], replica, shardLog, ackIndex, cancellationToken);
    }

    StreamShardingConfig StreamConfig(string streamName) =>
        assignmentTable.TryGetStreamConfig(streamName, out var config)
            ? config
            : throw new ArgumentException($"No stream '{streamName}' exists.", nameof(streamName));

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

    static RemontoireMessage ToMessage(LogEntry entry) {
        var headers = new Dictionary<string, string>();
        foreach (var header in entry.Headers)
            headers[Encoding.UTF8.GetString(header.Key.Span)] = Encoding.UTF8.GetString(header.Value.Span);

        return new RemontoireMessage(ShardId: 0, (long)entry.LogicalOffset, Encoding.UTF8.GetString(entry.PartitionKey.Span),
            entry.Payload.ToArray(), headers, ToDateTimeOffset(entry.TimestampMicros));
    }

    // The same conversion the gRPC client uses.
    static DateTimeOffset ToDateTimeOffset(ulong micros) => DateTimeOffset.UnixEpoch.AddTicks((long)micros * 10);
}
