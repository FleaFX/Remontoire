using Remontoire.Raft.V1;

namespace Remontoire.Raft;

/// <summary>
/// Abstracts the network used to contact peer replicas. Two implementations: the gRPC
/// transport (production) and the deterministic in-memory simulation (tests) — the latter is
/// the reason this interface exists at all.
/// </summary>
public interface IRaftTransport {
    /// <summary>Sends a vote request to <paramref name="peerId"/> and awaits the response.</summary>
    ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken cancellationToken = default);

    /// <summary>Sends an append-entries request (or heartbeat) to <paramref name="peerId"/>.</summary>
    ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken cancellationToken = default);

    /// <summary>Sends one snapshot chunk to <paramref name="peerId"/>.</summary>
    ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken cancellationToken = default);
}
