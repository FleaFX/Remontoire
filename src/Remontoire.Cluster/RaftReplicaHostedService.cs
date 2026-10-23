using Grpc.Net.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Remontoire.Engine;
using Remontoire.Meta.V1;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;

namespace Remontoire.Cluster;

/// <summary>
/// Starts every physical shard group this process hosts, from <see cref="RemontoireClusterOptions"/>,
/// on a <see cref="RemontoireNode"/>, sharing a single <see cref="RaftGrpcTransport"/> across every
/// group (and the meta-group, if hosted) so the same peer node never gets more than one channel
/// regardless of how many groups it's a peer in.
/// </summary>
/// <remarks>
/// Either way, this node ends up with a live <see cref="ShardAssignmentTable"/>: fed by its own
/// meta-group replica if it hosts one, or by a <see cref="ShardAssignmentWatcher"/> bootstrapped
/// from <see cref="RemontoireClusterOptions.MetaGroupSeedAddresses"/> otherwise.
/// </remarks>
sealed class RaftReplicaHostedService(
    IOptions<RemontoireClusterOptions> options, RemontoireNode node, LeaderAddressDirectory leaderAddresses,
    ShardAssignmentTable assignmentTable, ILoggerFactory loggerFactory, IEnumerable<RemontoireDeclaration> declarations)
    : IHostedService {
    static readonly TimeSpan RegistrationRetryDelay = TimeSpan.FromSeconds(1);

    readonly CancellationTokenSource _cts = new();
    Task? _groupRegistration;

    RaftGrpcTransport? _transport;
    ShardAssignmentWatcher? _tableWatcher;
    GrpcChannel? _watcherChannel;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken) {
        // Declarations wait for the meta group while the host starts; on a cluster node that needs
        // peers whose Raft endpoint listens only once every hosted service has started.
        if (declarations.Any())
            throw new InvalidOperationException(
                "A cluster node does not apply AddStream or AddConsumerGroup declarations: declare them from a client (AddClient), or call IRemontoireAdmin once the host has started.");

        var raftOptions = options.Value;

        var groupPeers = raftOptions.Groups.Select(group => group.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))));
        var metaPeers = raftOptions.MetaGroup?.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))).ToArray() ?? [];
        var allPeers = groupPeers.SelectMany(peers => peers).Concat(metaPeers).DistinctBy(peer => peer.NodeId).ToArray();

        // A redirect sends a client to the leader's client endpoint, which isn't its Raft address
        // when the host listens for peers and clients on separate ports.
        var allPeerOptions = raftOptions.Groups.SelectMany(group => group.Peers).Concat(raftOptions.MetaGroup?.Peers ?? []).DistinctBy(peer => peer.NodeId);
        foreach (var peer in allPeerOptions)
            leaderAddresses.Register(peer.NodeId, new Uri(peer.ClientAddress ?? peer.Address));

        // Every group (and the meta-group) shares the same heartbeat cadence (CreateConfig), so they'd
        // all resolve to the same RPC timeout regardless — computed once here, directly.
        var rpcTimeout = TimeSpan.FromMilliseconds(50) * 5;
        _transport = new RaftGrpcTransport(allPeers, rpcTimeout, raftOptions.ConfigureHttpHandler);

        foreach (var group in raftOptions.Groups) {
            var peers = group.Peers.Select(peer => new RaftGroupMember(peer.NodeId, new Uri(peer.Address))).ToArray();
            await node.OpenGroupAsync(CreateConfig(group.GroupId, group.NodeId, peers), group.DataDirectory, _transport, cancellationToken);
        }

        if (raftOptions.MetaGroup is { } metaOptions) {
            await node.OpenMetaGroupAsync(CreateConfig(RemontoireNode.MetaGroupId, metaOptions.NodeId, metaPeers), metaOptions.DataDirectory,
                _transport, cancellationToken);
            var transport = _transport;
            node.ForwardMetaProposalAsync = (leaderNodeId, request, ct) => transport.ProposeMetaAsync(leaderNodeId, request, ct);
        } else if (raftOptions.MetaGroupSeedAddresses is { Count: > 0 } seedAddresses) {
            _watcherChannel = GrpcChannel.ForAddress(seedAddresses[0], CreateWatcherChannelOptions(raftOptions.ConfigureHttpHandler));
            var client = new ShardAssignmentMeta.ShardAssignmentMetaClient(_watcherChannel);
            _tableWatcher = new ShardAssignmentWatcher(client, assignmentTable, logger: loggerFactory.CreateLogger("Remontoire.Sharding.ShardAssignmentWatcher"));
        }

        node.MarkStarted();

        if (raftOptions.MetaGroup is not null && raftOptions.Groups.Count > 0 && raftOptions.ClientAddress is { } clientAddress)
            _groupRegistration = Task.Run(() => RegisterGroupsAsync(raftOptions.Groups, new Uri(clientAddress), _cts.Token));
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken) {
        await _cts.CancelAsync();
        if (_groupRegistration is not null)
            await _groupRegistration;
        node.ForwardMetaProposalAsync = null;

        if (_tableWatcher is not null)
            await _tableWatcher.DisposeAsync();
        _watcherChannel?.Dispose();

        await node.StopAsync();

        _transport?.Dispose();
        _cts.Dispose();
    }

    // Clients and new streams find a data group through the meta log. A node with a meta-group
    // replica registers every group it hosts with the members its configuration names, itself at
    // its client address; again at every start, so a changed address takes effect. In the
    // background, retried: the meta group may not have a leader until the peers are up.
    async Task RegisterGroupsAsync(IReadOnlyList<RaftGroupOptions> groups, Uri clientAddress, CancellationToken cancellationToken) {
        var logger = loggerFactory.CreateLogger<RaftReplicaHostedService>();
        try {
            foreach (var group in groups) {
                var members = group.Peers
                    .Select(peer => new ShardGroupMember(peer.NodeId, new Uri(peer.ClientAddress ?? peer.Address)))
                    .Append(new ShardGroupMember(group.NodeId, clientAddress))
                    .OrderBy(member => member.NodeId, StringComparer.Ordinal)
                    .ToArray();

                while (!(assignmentTable.TryGetGroup(group.GroupId, out var registered) && registered.Members.SequenceEqual(members))) {
                    try {
                        await node.ProposeMetaAsync([new RegisterGroup(group.GroupId, members)], proposedBy: null, cancellationToken);
                    } catch (Exception ex) when (ex is not OperationCanceledException) {
                        logger.LogWarning(ex, "Could not register group {GroupId} in the meta log yet; retrying.", group.GroupId);
                        await Task.Delay(RegistrationRetryDelay, cancellationToken);
                    }
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — StopAsync cancels and awaits this.
        }
    }

    static RaftReplicaConfig CreateConfig(string groupId, string nodeId, IReadOnlyList<RaftGroupMember> peers) => new(
        GroupId: groupId,
        NodeId: nodeId,
        Peers: peers,
        HeartbeatInterval: TimeSpan.FromMilliseconds(50),
        ElectionTimeoutMin: TimeSpan.FromMilliseconds(250),
        ElectionTimeoutMax: TimeSpan.FromMilliseconds(500));

    // The host's hook on a fresh handler of its own: the channel disposes its handler, so it can
    // never be shared with the transport's.
    static GrpcChannelOptions CreateWatcherChannelOptions(Action<SocketsHttpHandler>? configureHttpHandler) {
        if (configureHttpHandler is null)
            return new GrpcChannelOptions();

        var handler = new SocketsHttpHandler();
        configureHttpHandler(handler);
        return new GrpcChannelOptions { HttpHandler = handler, DisposeHttpClient = true };
    }
}
