using System.Text.RegularExpressions;
using Remontoire.Sharding;

namespace Remontoire.Engine;

/// <summary>
/// The administration of a node, embedded or a cluster node: declares streams and consumer groups in
/// the meta log, and describes them from this node's <see cref="ShardAssignmentTable"/>. Serves
/// <see cref="IRemontoireAdmin"/> in-process and the admin endpoint of <c>Remontoire.Server</c>.
/// Declaring needs a meta-group replica on this node; describing works on every node.
/// </summary>
/// <remarks>
/// Every record a declaration proposes is idempotent, and whether a declaration conflicts is decided
/// on the table after its own records applied: two declarations racing on different nodes end up in
/// the same state, and the loser of a real conflict sees it.
/// </remarks>
/// <param name="node">Proposes to the meta-group leader.</param>
/// <param name="assignmentTable">This node's view of the meta log.</param>
/// <param name="nodeInfo">Whether this node can declare, and where a new stream goes.</param>
public sealed partial class NodeAdmin(RemontoireNode node, ShardAssignmentTable assignmentTable, RemontoireNodeInfo nodeInfo) : IRemontoireAdmin {
    // The only sharding a declaration creates; consumption reads virtual shard 0 only.
    const int VirtualShardCount = 1;
    const RoutingAlgorithm Routing = RoutingAlgorithm.XxHash3V1;

    static readonly TimeSpan OpenTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Whether <paramref name="name"/> can name a stream or a consumer group — and, embedded, a
    /// stream's group and its directory: 1 to 100 letters, digits, '.', '_' or '-', starting with a
    /// letter or digit.
    /// </summary>
    /// <param name="name">The candidate name.</param>
    /// <returns><see langword="true"/> for a valid name.</returns>
    internal static bool IsValidName(string name) => ValidName().IsMatch(name);

    [GeneratedRegex(@"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}\z")]
    private static partial Regex ValidName();

    /// <inheritdoc />
    Task IRemontoireAdmin.EnsureStreamAsync(string streamName, StreamOptions? options, CancellationToken cancellationToken) =>
        EnsureStreamAsync(streamName, options, proposedBy: null, cancellationToken);

    /// <inheritdoc />
    Task IRemontoireAdmin.EnsureConsumerGroupAsync(string streamName, string consumerGroup, ConsumerGroupOptions? options, CancellationToken cancellationToken) =>
        EnsureConsumerGroupAsync(streamName, consumerGroup, options, proposedBy: null, cancellationToken);

    /// <inheritdoc />
    public Task<StreamDescription?> GetStreamAsync(string streamName, CancellationToken cancellationToken = default) =>
        Task.FromResult(assignmentTable.DescribeStream(streamName));

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamDescription>> ListStreamsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(assignmentTable.DescribeStreams());

    /// <summary>
    /// <see cref="IRemontoireAdmin.EnsureStreamAsync"/>, with the proposer named in the audit trail.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="options">Its configuration; <see langword="null"/> for the defaults.</param>
    /// <param name="proposedBy">Who declares it; <see langword="null"/> for nobody in particular.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    public async Task EnsureStreamAsync(string streamName, StreamOptions? options, string? proposedBy, CancellationToken cancellationToken) {
        options ??= new StreamOptions();
        ValidateName(streamName, nameof(streamName));
        ValidateOptions(options);
        RequireMetaGroupReplica();
        ThrowOnConflict(streamName);

        if (!assignmentTable.TryGetAssignment(streamName, 0, out _)) {
            var (groups, groupId) = Place(streamName);
            await node.ProposeMetaAsync(StreamBootstrap.Records(streamName, VirtualShardCount, Routing, groups, [(0, groupId)]), proposedBy, cancellationToken);
            ThrowOnConflict(streamName); // another declaration may have created it first, otherwise
        }

        var policy = assignmentTable.GetRetentionPolicy(streamName);
        var changes = new List<MetaLogRecord>();
        if (policy.MaxRetention != options.MaxRetention || policy.MaxSizeBytesPerVirtualShard != options.MaxSizeBytesPerVirtualShard)
            changes.Add(new SetStreamRetentionPolicy(streamName, policy.AuditRetention, options.MaxRetention, options.MaxSizeBytesPerVirtualShard));
        if (policy.CheckpointInterval != options.CheckpointInterval || policy.CheckpointOffsetCount != options.CheckpointOffsetCount)
            changes.Add(new SetStreamCheckpointInterval(streamName, options.CheckpointInterval, options.CheckpointOffsetCount));
        if (changes.Count > 0)
            await node.ProposeMetaAsync(changes, proposedBy, cancellationToken);

        // Embedded, the stream's group opens on this node: accept messages before returning.
        if (nodeInfo.OwnGroupMember is not null && !await ConditionPoller.WaitUntilAsync(
                () => assignmentTable.TryGetAssignment(streamName, 0, out var assignment) && node.IsCaughtUp(assignment.GroupId),
                OpenTimeout, cancellationToken: cancellationToken))
            throw new TimeoutException($"Stream '{streamName}' did not open within {OpenTimeout.TotalSeconds} seconds; see the log.");
    }

    /// <summary>
    /// <see cref="IRemontoireAdmin.EnsureConsumerGroupAsync"/>, with the proposer named in the audit trail.
    /// </summary>
    /// <param name="streamName">The stream the group consumes.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <param name="options">Its configuration; <see langword="null"/> for the defaults.</param>
    /// <param name="proposedBy">Who declares it; <see langword="null"/> for nobody in particular.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    public async Task EnsureConsumerGroupAsync(string streamName, string consumerGroup, ConsumerGroupOptions? options, string? proposedBy, CancellationToken cancellationToken) {
        options ??= new ConsumerGroupOptions();
        ValidateName(streamName, nameof(streamName));
        ValidateName(consumerGroup, nameof(consumerGroup));
        if (!Enum.IsDefined(options.AckMode))
            throw new ArgumentOutOfRangeException(nameof(options), options.AckMode, "Not a valid ack mode.");
        RequireMetaGroupReplica();

        if (assignmentTable.TryGetConsumerGroupPolicy(streamName, consumerGroup, out var policy)
            && policy.Mode == options.AckMode && policy.Mandatory == options.Mandatory)
            return;

        await node.ProposeMetaAsync([new DeclareConsumerGroup(streamName, consumerGroup, options.AckMode, options.Mandatory)], proposedBy, cancellationToken);
    }

    // Embedded, every stream gets a group of its own, named after it. In a cluster, the registered
    // data group with the fewest virtual shards, ties broken by the smallest id.
    (IReadOnlyList<RegisterGroup> Groups, string GroupId) Place(string streamName) {
        if (nodeInfo.OwnGroupMember is { } member)
            return ([new RegisterGroup(streamName, [member])], streamName);

        var shardsPerGroup = assignmentTable.EnumerateAssignments().GroupBy(assignment => assignment.GroupId).ToDictionary(group => group.Key, group => group.Count());
        var groupId = assignmentTable.EnumerateGroups()
            .Select(group => group.GroupId)
            .OrderBy(id => shardsPerGroup.GetValueOrDefault(id))
            .ThenBy(id => id, StringComparer.Ordinal)
            .FirstOrDefault();

        // The data groups register themselves in the background when their nodes start.
        return groupId is null
            ? throw new RemontoireUnavailableException(RemontoireNode.MetaGroupId, "No data group is registered in the meta log yet.")
            : ([], groupId);
    }

    void ThrowOnConflict(string streamName) {
        if (assignmentTable.TryGetStreamConfig(streamName, out var config) && (config.VirtualShardCount != VirtualShardCount || config.RoutingAlgorithm != Routing))
            throw new RemontoireConflictException(
                $"Stream '{streamName}' exists with {config.VirtualShardCount} virtual shard(s) routed by {config.RoutingAlgorithm}; a declaration has {VirtualShardCount}, routed by {Routing}.");
    }

    void RequireMetaGroupReplica() {
        if (!nodeInfo.HostsMetaGroupReplica)
            throw new InvalidOperationException("This node hosts no meta-group replica: declare through a node that does, or through Remontoire.Client.");
    }

    static void ValidateName(string name, string parameterName) {
        if (!IsValidName(name))
            throw new ArgumentException(
                $"'{name}' is not a valid name: use 1 to 100 letters, digits, '.', '_' or '-', starting with a letter or digit.", parameterName);
    }

    static void ValidateOptions(StreamOptions options) {
        if (options.MaxRetention <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxRetention, $"{nameof(StreamOptions.MaxRetention)} must be positive.");
        if (options.MaxSizeBytesPerVirtualShard is <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxSizeBytesPerVirtualShard, $"{nameof(StreamOptions.MaxSizeBytesPerVirtualShard)} must be positive.");
        if (options.CheckpointInterval is { } interval && interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), interval, $"{nameof(StreamOptions.CheckpointInterval)} must be positive.");
        if (options.CheckpointOffsetCount is <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), options.CheckpointOffsetCount, $"{nameof(StreamOptions.CheckpointOffsetCount)} must be positive.");
    }
}
