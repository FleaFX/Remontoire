using Microsoft.Extensions.Options;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;

namespace Remontoire.Server;

/// <summary>
/// Composes and starts the one <see cref="RaftReplica"/> this process hosts, from the "Raft"
/// configuration section — <see cref="WalRaftLog"/> for the log, <see cref="FileRaftStateStore"/>
/// for the small durable state, <see cref="RaftGrpcTransport"/> for peer RPCs — and registers it
/// so <see cref="RaftTransportGrpcService"/> can dispatch inbound RPCs to it.
/// </summary>
sealed class RaftReplicaHostedService(IOptions<RaftServerOptions> options, RaftReplicaRegistry registry) : IHostedService {
    WalRaftLog? _log;
    RaftGrpcTransport? _transport;
    RaftReplica? _replica;

    public async Task StartAsync(CancellationToken cancellationToken) {
        var raftOptions = options.Value;
        var peers = raftOptions.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))).ToArray();

        var config = new RaftReplicaConfig(
            GroupId: raftOptions.GroupId,
            NodeId: raftOptions.NodeId,
            Peers: peers,
            HeartbeatInterval: TimeSpan.FromMilliseconds(50),
            ElectionTimeoutMin: TimeSpan.FromMilliseconds(250),
            ElectionTimeoutMax: TimeSpan.FromMilliseconds(500));

        _log = await WalRaftLog.OpenAsync(raftOptions.DataDirectory, cancellationToken: cancellationToken);
        var stateStore = new FileRaftStateStore(raftOptions.DataDirectory);
        _transport = new RaftGrpcTransport(peers, config.ResolvedRpcTimeout);

        _replica = new RaftReplica(stateStore, _log, _transport, config);
        await _replica.StartAsync(cancellationToken);
        registry.Register(_replica);
    }

    public async Task StopAsync(CancellationToken cancellationToken) {
        if (_replica is not null) {
            registry.Unregister(_replica.GroupId);
            await _replica.DisposeAsync();
        }

        _transport?.Dispose();

        if (_log is not null)
            await _log.DisposeAsync();
    }
}
