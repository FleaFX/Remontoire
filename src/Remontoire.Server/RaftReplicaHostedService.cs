using Grpc.Net.Client;
using Microsoft.Extensions.Options;
using Remontoire.Messaging;
using Remontoire.Meta.V1;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// Composes and starts every physical shard group this process hosts, from the "Raft"
/// configuration section — one <see cref="WalRaftLog"/>/<see cref="FileRaftStateStore"/>/
/// <see cref="RaftReplica"/>/<see cref="ShardLog"/>/<see cref="AckIndexApplier"/> composition per
/// group, sharing a single <see cref="RaftGrpcTransport"/> across every group (and the meta-group,
/// if hosted) so the same peer node never gets more than one channel regardless of how many
/// groups it's a peer in. Registers every group so <see cref="RaftTransportGrpcService"/> and
/// <see cref="Grpc.RemontoireClientGrpcService"/> can dispatch to it.
/// </summary>
/// <remarks>
/// Optionally also composes and starts this node's meta-group replica, if configured — the
/// meta-group holds zero virtual shards of its own, so it gets neither a <see cref="ShardLog"/>
/// nor a <see cref="MessagingGroupRegistry"/> registration. Either way, this node ends up with a
/// live <see cref="ShardAssignmentTable"/>: fed directly by a <see cref="ShardAssignmentTableApplier"/>
/// if this node hosts a meta-group member, or by a <see cref="ShardAssignmentWatcher"/> bootstrapped
/// from <see cref="RaftServerOptions.MetaGroupSeedAddresses"/> otherwise.
/// </remarks>
sealed class RaftReplicaHostedService(
    IOptions<RaftServerOptions> options, RaftReplicaRegistry registry, MessagingGroupRegistry messagingRegistry,
    LeaderAddressDirectory leaderAddresses, ShardAssignmentTable assignmentTable, MetaLogJournal metaLogJournal,
    MigrationAdmissionGate admissionGate)
    : IHostedService {
    readonly Dictionary<string, WalRaftLog> _logs = new();
    readonly Dictionary<string, RaftReplica> _replicas = new();
    readonly Dictionary<string, ShardLog> _shardLogs = new();
    readonly Dictionary<string, AckIndexApplier> _ackIndexAppliers = new();
    readonly Dictionary<string, AckCheckpointer> _ackCheckpointers = new();
    readonly Dictionary<string, RetentionEvaluator> _retentionEvaluators = new();
    RaftGrpcTransport? _transport;
    WalRaftLog? _metaLog;
    RaftReplica? _metaReplica;
    ShardAssignmentTableApplier? _metaTableApplier;
    ShardAssignmentWatcher? _tableWatcher;
    GrpcChannel? _watcherChannel;

    public async Task StartAsync(CancellationToken cancellationToken) {
        var raftOptions = options.Value;

        var groupPeers = raftOptions.Groups.Select(group => group.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))));
        var metaPeers = raftOptions.MetaGroup?.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))).ToArray() ?? [];
        var allPeers = groupPeers.SelectMany(peers => peers).Concat(metaPeers).DistinctBy(peer => peer.NodeId).ToArray();

        foreach (var peer in allPeers)
            leaderAddresses.Register(peer.NodeId, peer.Address);

        // Every group (and the meta-group) shares the same heartbeat cadence below, so they'd all
        // resolve to the same RPC timeout regardless — computed once here, directly, rather than
        // building a throwaway RaftReplicaConfig just to read it off.
        var rpcTimeout = TimeSpan.FromMilliseconds(50) * 5;
        _transport = new RaftGrpcTransport(allPeers, rpcTimeout);

        foreach (var group in raftOptions.Groups) {
            var peers = group.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))).ToArray();
            var config = new RaftReplicaConfig(
                GroupId: group.GroupId,
                NodeId: group.NodeId,
                Peers: peers,
                HeartbeatInterval: TimeSpan.FromMilliseconds(50),
                ElectionTimeoutMin: TimeSpan.FromMilliseconds(250),
                ElectionTimeoutMax: TimeSpan.FromMilliseconds(500));

            var log = await WalRaftLog.OpenAsync(group.DataDirectory, cancellationToken: cancellationToken);
            _logs[group.GroupId] = log; // tracked immediately — StopAsync must be able to reach it even if a later step here throws
            var stateStore = new FileRaftStateStore(group.DataDirectory);

            var replica = new RaftReplica(stateStore, log, _transport, config);
            await replica.StartAsync(cancellationToken);
            registry.Register(replica);
            _replicas[group.GroupId] = replica;

            var ackIndex = new AckIndex();

            // Looked up lazily, by groupId, at invocation time (each RetentionPassRequested tick,
            // hours after StartAsync returns) rather than captured directly — the RetentionEvaluator
            // this refers to isn't constructed until after ShardLog itself opens (its own
            // constructor needs an already-open ShardLog), so a direct closure over it here would
            // be circular. By the time this delegate is ever actually called, the dictionary entry
            // below is long since populated.
            var compactionPolicy = new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null,
                GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(_retentionEvaluators[group.GroupId].SafeToPruneWatermark));

            // Re-resolved every tick rather than fixed at construction — see ResolveStreamNameForGroup's
            // own remarks: this group's assignment isn't known yet at this point in StartAsync (the
            // meta-group/watcher section below hasn't even started), so a value captured here would
            // stay null forever instead of self-healing once the table catches up.
            var retentionPolicy = new RetentionPolicy(
                GetMaxTotalBytesPerVirtualShard: () => ResolveStreamNameForGroup(group.GroupId) is { } streamName
                    ? assignmentTable.GetRetentionPolicy(streamName).MaxSizeBytesPerVirtualShard : null,
                IsAdmissionPaused: () => admissionGate.IsPaused(group.GroupId));

            var shardLog = await ShardLog.OpenAsync(group.DataDirectory, replica.ReadCommittedAsync,
                compactionPolicy: compactionPolicy, retentionPolicy: retentionPolicy, cancellationToken: cancellationToken);
            _shardLogs[group.GroupId] = shardLog;

            var ackIndexApplier = new AckIndexApplier(shardLog, ackIndex);
            messagingRegistry.Register(group.GroupId, shardLog, ackIndex);
            _ackIndexAppliers[group.GroupId] = ackIndexApplier;

            _ackCheckpointers[group.GroupId] = new AckCheckpointer(new AckCheckpointerOptions(
                AckIndex: ackIndex,
                ProposeCheckpointAsync: (consumerGroup, watermark, ct) => replica.ProposeAsync(new AckCheckpointRequest(consumerGroup, watermark), ct).AsTask(),
                IsLeader: () => replica.IsLeader,
                IsCheckpointMode: consumerGroup => ResolveStreamNameForGroup(group.GroupId) is { } streamName
                    && assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup).Mode == AckMode.Checkpoint,
                GetCheckpointThresholds: () => ResolveStreamNameForGroup(group.GroupId) is { } streamName
                    ? (assignmentTable.GetRetentionPolicy(streamName).CheckpointInterval, assignmentTable.GetRetentionPolicy(streamName).CheckpointOffsetCount)
                    : (null, null),
                IsAdmissionPaused: () => admissionGate.IsPaused(group.GroupId)));

            _retentionEvaluators[group.GroupId] = new RetentionEvaluator(new RetentionEvaluatorOptions(
                ShardLog: shardLog, AckIndex: ackIndex,
                IsMandatory: consumerGroup => ResolveStreamNameForGroup(group.GroupId) is not { } streamName
                    || assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup).Mandatory,
                GetMaxRetention: () => ResolveStreamNameForGroup(group.GroupId) is { } streamName ? assignmentTable.GetRetentionPolicy(streamName).MaxRetention : TimeSpan.MaxValue,
                ForwardToDeadLetterAsync: (request, ct) => ForwardToDeadLetterAsync(ResolveStreamNameForGroup(group.GroupId), request, ct),
                IsAdmissionPaused: () => admissionGate.IsPaused(group.GroupId),
                IsLeader: () => replica.IsLeader));
        }

        if (raftOptions.MetaGroup is { } metaOptions) {
            var metaConfig = new RaftReplicaConfig(
                GroupId: "__meta__",
                NodeId: metaOptions.NodeId,
                Peers: metaPeers,
                HeartbeatInterval: TimeSpan.FromMilliseconds(50),
                ElectionTimeoutMin: TimeSpan.FromMilliseconds(250),
                ElectionTimeoutMax: TimeSpan.FromMilliseconds(500));

            _metaLog = await WalRaftLog.OpenAsync(metaOptions.DataDirectory, cancellationToken: cancellationToken);
            var metaStateStore = new FileRaftStateStore(metaOptions.DataDirectory);

            _metaReplica = new RaftReplica(metaStateStore, _metaLog, _transport, metaConfig);
            await _metaReplica.StartAsync(cancellationToken);
            registry.Register(_metaReplica);

            _metaTableApplier = new ShardAssignmentTableApplier(_metaReplica, assignmentTable, metaLogJournal);
        } else if (raftOptions.MetaGroupSeedAddresses is { Count: > 0 } seedAddresses) {
            _watcherChannel = GrpcChannel.ForAddress(seedAddresses[0]);
            var client = new ShardAssignmentMeta.ShardAssignmentMetaClient(_watcherChannel);
            _tableWatcher = new ShardAssignmentWatcher(client, assignmentTable);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken) {
        if (_tableWatcher is not null)
            await _tableWatcher.DisposeAsync();
        _watcherChannel?.Dispose();

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

        foreach (var shardLog in _shardLogs.Values)
            await shardLog.DisposeAsync();

        foreach (var (groupId, replica) in _replicas) {
            registry.Unregister(groupId);
            await replica.DisposeAsync();
        }

        _transport?.Dispose();

        foreach (var log in _logs.Values)
            await log.DisposeAsync();
    }

    const string DeadLetterStreamSuffix = ".__deadletter__";

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
