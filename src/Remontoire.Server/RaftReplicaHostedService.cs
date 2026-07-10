using Microsoft.Extensions.Options;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// Composes and starts the one <see cref="RaftReplica"/> this process hosts, from the "Raft"
/// configuration section — <see cref="WalRaftLog"/> for the log, <see cref="FileRaftStateStore"/>
/// for the small durable state, <see cref="RaftGrpcTransport"/> for peer RPCs — and registers it
/// so <see cref="RaftTransportGrpcService"/> can dispatch inbound RPCs to it. Also composes the
/// <see cref="ShardLog"/> the replica's committed stream feeds and the <see cref="AckIndex"/>
/// maintained from it, registering both so <see cref="Grpc.RemontoireClientGrpcService"/> can
/// serve the client-facing RPCs.
/// </summary>
/// <remarks>
/// Optionally also composes and starts this node's meta-group replica, if configured. The
/// meta-group holds zero virtual shards of its own — no <see cref="ShardLog"/>, no
/// <see cref="AckIndex"/>, no <see cref="MessagingGroupRegistry"/> registration — but it shares
/// the same <see cref="RaftGrpcTransport"/> instance as the data group: transport construction
/// first merges every peer across both groups, deduplicated by node id, so the same peer node
/// never gets more than one channel regardless of how many groups it's a peer in.
/// </remarks>
sealed class RaftReplicaHostedService(
    IOptions<RaftServerOptions> options, RaftReplicaRegistry registry, MessagingGroupRegistry messagingRegistry, LeaderAddressDirectory leaderAddresses)
    : IHostedService {
    WalRaftLog? _log;
    RaftGrpcTransport? _transport;
    RaftReplica? _replica;
    ShardLog? _shardLog;
    AckIndexApplier? _ackIndexApplier;
    WalRaftLog? _metaLog;
    RaftReplica? _metaReplica;

    public async Task StartAsync(CancellationToken cancellationToken) {
        var raftOptions = options.Value;
        var peers = raftOptions.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))).ToArray();
        var metaPeers = raftOptions.MetaGroup?.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))).ToArray() ?? [];

        foreach (var peer in peers.Concat(metaPeers).DistinctBy(peer => peer.NodeId))
            leaderAddresses.Register(peer.NodeId, peer.Address);

        var config = new RaftReplicaConfig(
            GroupId: raftOptions.GroupId,
            NodeId: raftOptions.NodeId,
            Peers: peers,
            HeartbeatInterval: TimeSpan.FromMilliseconds(50),
            ElectionTimeoutMin: TimeSpan.FromMilliseconds(250),
            ElectionTimeoutMax: TimeSpan.FromMilliseconds(500));

        _log = await WalRaftLog.OpenAsync(raftOptions.DataDirectory, cancellationToken: cancellationToken);
        var stateStore = new FileRaftStateStore(raftOptions.DataDirectory);

        var allPeers = peers.Concat(metaPeers).DistinctBy(peer => peer.NodeId).ToArray();
        _transport = new RaftGrpcTransport(allPeers, config.ResolvedRpcTimeout);

        _replica = new RaftReplica(stateStore, _log, _transport, config);
        await _replica.StartAsync(cancellationToken);
        registry.Register(_replica);

        var ackIndex = new AckIndex();
        _shardLog = await ShardLog.OpenAsync(raftOptions.DataDirectory, _replica.ReadCommittedAsync,
            compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())),
            cancellationToken: cancellationToken);
        _ackIndexApplier = new AckIndexApplier(_shardLog, ackIndex);

        messagingRegistry.Register(raftOptions.GroupId, _shardLog, ackIndex);

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
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken) {
        if (_metaReplica is not null) {
            registry.Unregister(_metaReplica.GroupId);
            await _metaReplica.DisposeAsync();
        }

        if (_metaLog is not null)
            await _metaLog.DisposeAsync();

        if (_replica is not null)
            messagingRegistry.Unregister(_replica.GroupId);

        if (_ackIndexApplier is not null)
            await _ackIndexApplier.DisposeAsync();

        if (_shardLog is not null)
            await _shardLog.DisposeAsync();

        if (_replica is not null) {
            registry.Unregister(_replica.GroupId);
            await _replica.DisposeAsync();
        }

        _transport?.Dispose();

        if (_log is not null)
            await _log.DisposeAsync();
    }
}
