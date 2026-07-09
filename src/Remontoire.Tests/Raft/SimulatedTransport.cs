using Remontoire.Raft.V1;

namespace Remontoire.Raft;

/// <summary>
/// One node's view of the network in a <see cref="SimulatedCluster"/> — every outbound RPC is
/// handed to the cluster, which decides drop/delay/partition and, if the message survives,
/// delivers it as a direct, in-process call into the target <see cref="RaftReplica"/>.
/// </summary>
sealed class SimulatedTransport(string nodeId, SimulatedCluster cluster) : IRaftTransport {
    public async ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken cancellationToken = default) =>
        await cluster.DeliverAsync(nodeId, peerId, request, static (replica, req, ct) => replica.ReceiveVoteRequestAsync(req, ct), cancellationToken);

    public async ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken cancellationToken = default) =>
        await cluster.DeliverAsync(nodeId, peerId, request, static (replica, req, ct) => replica.ReceiveAppendEntriesAsync(req, ct), cancellationToken);

    public async ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken cancellationToken = default) =>
        await cluster.DeliverAsync(nodeId, peerId, request, static (replica, req, ct) => replica.ReceiveInstallSnapshotAsync(req, ct), cancellationToken);
}
