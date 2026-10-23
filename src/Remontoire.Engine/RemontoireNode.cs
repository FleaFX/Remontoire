using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

/// <summary>
/// The shard groups one process hosts: per data group one <see cref="WalRaftLog"/>/<see cref="FileRaftStateStore"/>/
/// <see cref="RaftReplica"/>/<see cref="ShardLog"/>/<see cref="AckIndexApplier"/> composition with its
/// <see cref="AckCheckpointer"/> and <see cref="RetentionEvaluator"/>, and optionally the meta-group
/// replica that feeds the <see cref="ShardAssignmentTable"/>. Registers every group so the gRPC
/// services and the in-process producer and consumer can dispatch to it. Shared by the embedded
/// engine (groups without peers) and a cluster node (groups with peers): the caller decides each
/// group's Raft configuration and transport.
/// </summary>
/// <remarks>
/// The meta-group holds zero virtual shards of its own, so it gets neither a <see cref="ShardLog"/>
/// nor a <see cref="MessagingGroupRegistry"/> registration.
/// </remarks>
/// <param name="registry">Where every replica is registered.</param>
/// <param name="messagingRegistry">Where every data group's shard log, ack index and retention evaluator are registered.</param>
/// <param name="assignmentTable">The table the meta-group feeds, and the source of every stream's policies.</param>
/// <param name="metaLogJournal">Receives every committed meta-log record, for the shard-assignment feed.</param>
/// <param name="admissionGate">Which groups pause admission during a reshard's cutover.</param>
/// <param name="metrics">Records the WAL fsync and segment compaction durations.</param>
/// <param name="loggerFactory">Creates the loggers of every component.</param>
public sealed class RemontoireNode(
    RaftReplicaRegistry registry,
    MessagingGroupRegistry messagingRegistry,
    ShardAssignmentTable assignmentTable,
    MetaLogJournal metaLogJournal,
    MigrationAdmissionGate admissionGate,
    RemontoireMetrics metrics,
    ILoggerFactory loggerFactory
) {
    /// <summary>
    /// The meta-group's well-known group id.
    /// </summary>
    public const string MetaGroupId = "__meta__";

    /// <summary>
    /// The header that names who proposed a meta-log record, for the audit trail.
    /// </summary>
    public const string ProposedByHeaderKey = "proposed-by";

    static readonly TimeSpan MetaProposeTimeout = TimeSpan.FromSeconds(10);
    static readonly TimeSpan MetaProposeRetryDelay = TimeSpan.FromMilliseconds(50);

    const string DeadLetterStreamSuffix = ".__deadletter__";

    // Concurrent: the embedded engine opens groups while the retention and checkpoint loops of
    // groups opened earlier already read these.
    readonly ConcurrentDictionary<string, WalRaftLog> _logs = new();
    readonly ConcurrentDictionary<string, RaftReplica> _replicas = new();
    readonly ConcurrentDictionary<string, ShardLog> _shardLogs = new();
    readonly ConcurrentDictionary<string, AckIndex> _ackIndexes = new();
    readonly ConcurrentDictionary<string, AckIndexApplier> _ackIndexAppliers = new();
    readonly ConcurrentDictionary<string, AckCheckpointer> _ackCheckpointers = new();
    readonly ConcurrentDictionary<string, RetentionEvaluator> _retentionEvaluators = new();
    readonly ConcurrentDictionary<string, string> _dataDirectories = new();
    WalRaftLog? _metaLog;
    RaftReplica? _metaReplica;
    ShardAssignmentTableApplier? _metaTableApplier;
    volatile bool _isStarted;

    /// <summary>
    /// How often each data group's <see cref="RetentionEvaluator"/> runs; <see langword="null"/>
    /// keeps its default. Set before opening groups.
    /// </summary>
    internal TimeSpan? RetentionTickInterval { get; set; }

    /// <summary>
    /// Forwards one meta-log proposal to the node with the given id, the meta-group leader as this
    /// node knows it, and returns the result once committed there. Throws
    /// <see cref="NotLeaderException"/> when that node does not lead, or cannot be reached. Set by a
    /// cluster node that hosts a meta-group replica; <see langword="null"/> embedded, where the
    /// replica always leads.
    /// </summary>
    public Func<string, AppendRequest, CancellationToken, Task<ProposeResult>>? ForwardMetaProposalAsync { get; set; }

    /// <summary>
    /// Whether a data group with id <paramref name="groupId"/> is open on this node.
    /// </summary>
    /// <param name="groupId">The group id.</param>
    /// <returns><see langword="true"/> once opening the group has started here.</returns>
    public bool HostsGroup(string groupId) => _replicas.ContainsKey(groupId);

    /// <summary>
    /// Whether <paramref name="groupId"/>'s replica leads its group, the group is registered, and its
    /// ack index reflects every committed record — after a restart, only then does a consumer start
    /// from the right offset.
    /// </summary>
    /// <param name="groupId">The group id.</param>
    /// <returns><see langword="true"/> once the group serves from up-to-date state.</returns>
    public bool IsCaughtUp(string groupId) =>
        _replicas.TryGetValue(groupId, out var replica) && replica.IsLeader
        && _ackIndexAppliers.TryGetValue(groupId, out var applier) && applier.AppliedIndex >= replica.CommitIndex
        && messagingRegistry.TryGet(groupId, out _);

    /// <summary>
    /// Whether the meta-group replica leads its group and the <see cref="ShardAssignmentTable"/>
    /// reflects every committed meta-log record; <see langword="false"/> without a meta-group replica.
    /// </summary>
    public bool IsMetaCaughtUp =>
        _metaReplica is { IsLeader: true } replica && _metaTableApplier is { } applier && applier.AppliedIndex >= replica.CommitIndex;

    /// <summary>
    /// Whether the hosting service finished starting this node and hasn't begun stopping it. An
    /// embedded node starts only once every group has caught up.
    /// </summary>
    public bool IsStarted => _isStarted;

    /// <summary>
    /// The data directory of every group opened here, the meta-group's included.
    /// </summary>
    public IReadOnlyCollection<string> DataDirectories => _dataDirectories.Values.ToArray();

    /// <summary>
    /// Marks this node started; the hosting service calls it at the end of its own start.
    /// </summary>
    public void MarkStarted() => _isStarted = true;

    /// <summary>
    /// Proposes <paramref name="records"/> to the meta-group leader, in order: directly when this
    /// node's meta-group replica leads, otherwise through <see cref="ForwardMetaProposalAsync"/>.
    /// Retries while no leader is known, for up to ten seconds per record. Completes once this
    /// node's <see cref="ShardAssignmentTable"/> reflects the last record.
    /// </summary>
    /// <param name="records">The records to propose; each must be idempotent, since a retried proposal may commit twice.</param>
    /// <param name="proposedBy">Who proposes them, for the audit trail; <see langword="null"/> for nobody in particular.</param>
    /// <param name="cancellationToken">Cancels the wait; a proposal already sent may still commit.</param>
    /// <exception cref="InvalidOperationException">This node hosts no meta-group replica.</exception>
    /// <exception cref="RemontoireUnavailableException">No leader accepted a record, or this node did not apply it, in time.</exception>
    public async Task ProposeMetaAsync(IEnumerable<MetaLogRecord> records, string? proposedBy, CancellationToken cancellationToken) {
        IReadOnlyList<Header> headers = proposedBy is null
            ? []
            : [new Header(Encoding.UTF8.GetBytes(ProposedByHeaderKey), Encoding.UTF8.GetBytes(proposedBy))];

        ulong? lastVersion = null;
        foreach (var record in records)
            lastVersion = (await ProposeOnMetaLeaderAsync(new AppendRequest(Array.Empty<byte>(), headers, MetaLogRecord.Encode(record)), cancellationToken)).LogicalOffset;

        if (lastVersion is { } version
            && !await ConditionPoller.WaitUntilAsync(() => metaLogJournal.Contains(version), MetaProposeTimeout, cancellationToken: cancellationToken))
            throw new RemontoireUnavailableException(MetaGroupId, "The meta log committed the records, but this node did not apply them in time.");
    }

    async Task<ProposeResult> ProposeOnMetaLeaderAsync(AppendRequest request, CancellationToken cancellationToken) {
        var deadline = DateTime.UtcNow + MetaProposeTimeout;
        var attempts = 0;
        Exception? lastFailure = null;
        while (true) {
            if (!registry.TryGet(MetaGroupId, out var metaReplica))
                throw new InvalidOperationException("This node hosts no meta-group replica.");

            attempts++;
            try {
                if (metaReplica.IsLeader)
                    return await metaReplica.ProposeAsync(request, cancellationToken);
                if (metaReplica.LeaderHint is { } leader && ForwardMetaProposalAsync is { } forward)
                    return await forward(leader, request, cancellationToken);
            } catch (NotLeaderException ex) {
                lastFailure = ex; // leadership moved, or the leader isn't reachable: ask again after the delay
            }

            if (DateTime.UtcNow >= deadline)
                throw new RemontoireUnavailableException(MetaGroupId, attempts, lastFailure);
            await Task.Delay(MetaProposeRetryDelay, cancellationToken);
        }
    }

    /// <summary>
    /// Opens one data group in <paramref name="dataDirectory"/> (creating it if necessary), starts its
    /// replica, and registers it.
    /// </summary>
    /// <param name="config">The group's Raft configuration; its <see cref="RaftReplicaConfig.GroupId"/> names the group.</param>
    /// <param name="dataDirectory">Where the group keeps its WAL, Raft state and segments.</param>
    /// <param name="transport">How the replica reaches its peers.</param>
    /// <param name="cancellationToken">Cancels opening.</param>
    public async Task OpenGroupAsync(RaftReplicaConfig config, string dataDirectory, IRaftTransport transport, CancellationToken cancellationToken = default) {
        var groupId = config.GroupId;

        // Every background loop started below (RaftReplica's actor, ShardLog's actor/compaction/
        // size-prune workers, AckCheckpointer, RetentionEvaluator) is a Task.Run — each captures
        // this scope's ambient ExecutionContext at its own Task.Run call, so it keeps carrying
        // NodeId/GroupId on every log line for its whole lifetime, entirely independent of
        // this scope disposing when this method returns.
        using var groupScope = loggerFactory.CreateLogger<RemontoireNode>().BeginScope(new Dictionary<string, object> {
            ["NodeId"] = config.NodeId, ["GroupId"] = groupId,
        });

        _dataDirectories[groupId] = dataDirectory;
        var log = await WalRaftLog.OpenAsync(dataDirectory,
            onFsyncDurationMeasured: duration => metrics.RecordWalFsync(groupId, duration),
            cancellationToken: cancellationToken);
        _logs[groupId] = log; // tracked immediately — StopAsync must be able to reach it even if a later step here throws
        var stateStore = new FileRaftStateStore(dataDirectory);

        var replica = new RaftReplica(stateStore, log, transport, config, logger: loggerFactory.CreateLogger("Remontoire.Raft.RaftReplica"));
        await replica.StartAsync(cancellationToken);
        registry.Register(replica);
        _replicas[groupId] = replica;

        var ackIndex = new AckIndex();
        _ackIndexes[groupId] = ackIndex;

        // Looked up lazily, by groupId, at invocation time (each RetentionPassRequested tick,
        // hours after this method returns) rather than captured directly — the RetentionEvaluator
        // this refers to isn't constructed until after ShardLog itself opens (its own
        // constructor needs an already-open ShardLog), so a direct closure over it here would
        // be circular. By the time this delegate is ever actually called, the dictionary entry
        // below is long since populated.
        var compactionPolicy = new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null,
            GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(_retentionEvaluators[groupId].SafeToPruneWatermark),
            OnCompactionDurationMeasured: duration => metrics.RecordSegmentCompaction(groupId, duration));

        // Re-resolved every tick rather than fixed at construction — see ResolveStreamNameForGroup's
        // own remarks: this group's assignment may not be known yet at this point (the meta-group or
        // watcher may not have caught up), so a value captured here would stay null forever instead
        // of self-healing once the table catches up.
        var retentionPolicy = new RetentionPolicy(
            GetMaxTotalBytesPerVirtualShard: () => ResolveStreamNameForGroup(groupId) is { } streamName
                ? assignmentTable.GetRetentionPolicy(streamName).MaxSizeBytesPerVirtualShard : null,
            IsAdmissionPaused: () => admissionGate.IsPaused(groupId));

        var shardLog = await ShardLog.OpenAsync(dataDirectory, replica.ReadCommittedAsync,
            compactionPolicy: compactionPolicy, retentionPolicy: retentionPolicy,
            logger: loggerFactory.CreateLogger("Remontoire.Storage.ShardLog"), cancellationToken: cancellationToken);
        _shardLogs[groupId] = shardLog;

        var ackIndexApplier = new AckIndexApplier(shardLog, ackIndex);
        _ackIndexAppliers[groupId] = ackIndexApplier;

        _ackCheckpointers[groupId] = new AckCheckpointer(new AckCheckpointerOptions(
            AckIndex: ackIndex,
            ProposeCheckpointAsync: (consumerGroup, watermark, ct) => replica.ProposeAsync(new AckCheckpointRequest(consumerGroup, watermark), ct).AsTask(),
            IsLeader: () => replica.IsLeader,
            IsCheckpointMode: consumerGroup => ResolveStreamNameForGroup(groupId) is { } streamName
                && assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup).Mode == AckMode.Checkpoint,
            GetCheckpointThresholds: () => ResolveStreamNameForGroup(groupId) is { } streamName
                ? (assignmentTable.GetRetentionPolicy(streamName).CheckpointInterval, assignmentTable.GetRetentionPolicy(streamName).CheckpointOffsetCount)
                : (null, null),
            IsAdmissionPaused: () => admissionGate.IsPaused(groupId),
            Logger: loggerFactory.CreateLogger("Remontoire.Messaging.AckCheckpointer")));

        var retentionEvaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
            ShardLog: shardLog, AckIndex: ackIndex,
            IsMandatory: consumerGroup => ResolveStreamNameForGroup(groupId) is not { } streamName
                || assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup).Mandatory,
            GetMaxRetention: () => ResolveStreamNameForGroup(groupId) is { } streamName ? assignmentTable.GetRetentionPolicy(streamName).MaxRetention : TimeSpan.MaxValue,
            ForwardToDeadLetterAsync: (request, ct) => ForwardToDeadLetterAsync(ResolveStreamNameForGroup(groupId), request, ct),
            IsAdmissionPaused: () => admissionGate.IsPaused(groupId),
            IsLeader: () => replica.IsLeader,
            TickInterval: RetentionTickInterval,
            Logger: loggerFactory.CreateLogger("Remontoire.Messaging.RetentionEvaluator"),
            GetDeclaredConsumerGroups: () => ResolveStreamNameForGroup(groupId) is { } streamName
                ? assignmentTable.DeclaredConsumerGroups(streamName)
                : []));
        _retentionEvaluators[groupId] = retentionEvaluator;

        messagingRegistry.Register(groupId, shardLog, ackIndex, retentionEvaluator);
    }

    /// <summary>
    /// Opens this node's meta-group replica in <paramref name="dataDirectory"/> (creating it if
    /// necessary), starts it, registers it, and starts feeding the <see cref="ShardAssignmentTable"/>
    /// and the <see cref="MetaLogJournal"/> from it.
    /// </summary>
    /// <param name="config">The meta-group's Raft configuration; its group id is <see cref="MetaGroupId"/>.</param>
    /// <param name="dataDirectory">Where the meta-group keeps its WAL and Raft state.</param>
    /// <param name="transport">How the replica reaches its peers.</param>
    /// <param name="cancellationToken">Cancels opening.</param>
    public async Task OpenMetaGroupAsync(RaftReplicaConfig config, string dataDirectory, IRaftTransport transport, CancellationToken cancellationToken = default) {
        using var metaScope = loggerFactory.CreateLogger<RemontoireNode>().BeginScope(new Dictionary<string, object> {
            ["NodeId"] = config.NodeId, ["GroupId"] = MetaGroupId,
        });

        _dataDirectories[MetaGroupId] = dataDirectory;
        _metaLog = await WalRaftLog.OpenAsync(dataDirectory,
            onFsyncDurationMeasured: duration => metrics.RecordWalFsync(MetaGroupId, duration),
            cancellationToken: cancellationToken);
        var metaStateStore = new FileRaftStateStore(dataDirectory);

        _metaReplica = new RaftReplica(metaStateStore, _metaLog, transport, config, logger: loggerFactory.CreateLogger("Remontoire.Raft.RaftReplica"));
        await _metaReplica.StartAsync(cancellationToken);
        registry.Register(_metaReplica);

        _metaTableApplier = new ShardAssignmentTableApplier(_metaReplica, assignmentTable, metaLogJournal);
    }

    /// <summary>
    /// Marks this node no longer started, then stops and closes everything opened here, the
    /// meta-group first, each data group in dependency order, and unregisters it.
    /// </summary>
    public async Task StopAsync() {
        _isStarted = false;

        if (_metaTableApplier is not null)
            await _metaTableApplier.DisposeAsync();

        if (_metaReplica is not null) {
            registry.Unregister(_metaReplica.GroupId);
            await _metaReplica.DisposeAsync();
        }

        if (_metaLog is not null)
            await _metaLog.DisposeAsync();

        // Both post to the Raft replica (AckCheckpointer) or read from the ShardLog (RetentionEvaluator)
        // they were built against — stop before either underlying dependency does, same reason
        // _ackIndexAppliers already stops before _shardLogs below.
        foreach (var checkpointer in _ackCheckpointers.Values)
            await checkpointer.DisposeAsync();
        foreach (var evaluator in _retentionEvaluators.Values)
            await evaluator.DisposeAsync();

        foreach (var (groupId, ackIndexApplier) in _ackIndexAppliers) {
            messagingRegistry.Unregister(groupId);
            await ackIndexApplier.DisposeAsync();
        }

        // Stops after AckIndexApplier (its only production feeder of Apply calls) and after
        // AckCheckpointer/RetentionEvaluator above (both call into it during their own shutdown).
        foreach (var ackIndex in _ackIndexes.Values)
            await ackIndex.DisposeAsync();

        foreach (var shardLog in _shardLogs.Values)
            await shardLog.DisposeAsync();

        foreach (var (groupId, replica) in _replicas) {
            registry.Unregister(groupId);
            await replica.DisposeAsync();
        }

        foreach (var log in _logs.Values)
            await log.DisposeAsync();
    }

    // A physical group carries no stream name of its own (neither WalRecord nor RaftGroupOptions
    // does) — the only way to answer "which stream does this group serve" is a reverse scan of the
    // shard-assignment table's own assignments. Deliberately re-run on every call rather than
    // cached: this group's assignment may not be known yet at the moment a caller first asks (the
    // meta-group/watcher hasn't necessarily caught up), so a cached answer could stay stale
    // forever instead of self-healing once the table catches up. Cheap enough for that: an
    // in-memory scan, called at most once per periodic tick, never per request.
    string? ResolveStreamNameForGroup(string groupId) =>
        PickPrimaryStreamName(assignmentTable.EnumerateAssignments().Where(a => a.GroupId == groupId));

    // A group can end up serving more than one stream once a dead-letter stream is provisioned
    // onto the same physical group as its own source (§4.4's own v1 convention) — a plain
    // FirstOrDefault would then resolve to whichever one ConcurrentDictionary's unordered
    // enumeration happens to yield first, silently applying the wrong stream's retention/
    // checkpoint policy. Prefer the real source stream over its own dead-letter shadow; extracted
    // as its own static method so the tie-break rule is directly testable, independent of the
    // table's actual iteration order.
    internal static string? PickPrimaryStreamName(IEnumerable<VirtualShardAssignment> candidates) {
        string? first = null;
        foreach (var candidate in candidates) {
            first ??= candidate.StreamName;
            if (!candidate.StreamName.EndsWith(DeadLetterStreamSuffix))
                return candidate.StreamName;
        }

        return first;
    }

    // The one place that can resolve where a dead-letter stream's own virtual shard currently
    // lives — RetentionEvaluator (Remontoire.Messaging) has no reference to Remontoire.Sharding to
    // do this itself. Returns false (never forwards) when the destination isn't hosted on this
    // node, or the dead-letter stream was never provisioned — both explicitly accepted v1 gaps
    // (§4.4/§6.3), not a crash — so the caller knows no copy was actually made and must not treat
    // the source message as safe to prune.
    async Task<bool> ForwardToDeadLetterAsync(string? streamName, AppendRequest request, CancellationToken cancellationToken) {
        if (streamName is null)
            return false;

        var deadLetterStreamName = $"{streamName}{DeadLetterStreamSuffix}";
        if (!assignmentTable.TryGetStreamConfig(deadLetterStreamName, out var config))
            return false;

        var virtualShardIndex = ShardRouter.GetVirtualShardIndex(request.PartitionKey.Span, config.VirtualShardCount, config.RoutingAlgorithm);
        if (!assignmentTable.TryGetAssignment(deadLetterStreamName, virtualShardIndex, out var assignment) ||
            !_replicas.TryGetValue(assignment.GroupId, out var replica))
            return false;

        await replica.ProposeAsync(request, cancellationToken);
        return true;
    }
}
