using Remontoire.Raft;
using Remontoire.Raft.V1;

namespace Remontoire.Engine;

/// <summary>
/// The transport of a group without peers: such a replica is its own quorum and never sends a
/// Raft RPC, so every call here is a bug.
/// </summary>
sealed class NoPeersRaftTransport : IRaftTransport {
    /// <summary>
    /// The one instance; it holds no state.
    /// </summary>
    public static readonly NoPeersRaftTransport Instance = new();

    /// <inheritdoc />
    public ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken cancellationToken = default) =>
        throw NoPeers(peerId);

    /// <inheritdoc />
    public ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken cancellationToken = default) =>
        throw NoPeers(peerId);

    /// <inheritdoc />
    public ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken cancellationToken = default) =>
        throw NoPeers(peerId);

    static InvalidOperationException NoPeers(string peerId) => new($"A group without peers has no peer '{peerId}' to contact.");
}
