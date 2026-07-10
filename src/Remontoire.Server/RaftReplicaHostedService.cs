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
    LeaderAddressDirectory leaderAddresses, ShardAssignmentTable assignmentTable, MetaLogJournal metaLogJournal)
    : IHostedService {
    readonly Dictionary<string, WalRaftLog> _logs = new();
    readonly Dictionary<string, RaftReplica> _replicas = new();
    readonly Dictionary<string, ShardLog> _shardLogs = new();
    readonly Dictionary<string, AckIndexApplier> _ackIndexAppliers = new();
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
            var stateStore = new FileRaftStateStore(group.DataDirectory);

            var replica = new RaftReplica(stateStore, log, _transport, config);
            await replica.StartAsync(cancellationToken);
            registry.Register(replica);

            var ackIndex = new AckIndex();
            var shardLog = await ShardLog.OpenAsync(group.DataDirectory, replica.ReadCommittedAsync,
                compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())),
                cancellationToken: cancellationToken);
            var ackIndexApplier = new AckIndexApplier(shardLog, ackIndex);

            messagingRegistry.Register(group.GroupId, shardLog, ackIndex);

            _logs[group.GroupId] = log;
            _replicas[group.GroupId] = replica;
            _shardLogs[group.GroupId] = shardLog;
            _ackIndexAppliers[group.GroupId] = ackIndexApplier;
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
}
