using Remontoire.Raft.V1;

namespace Remontoire.Raft;

/// <summary>
/// Records every outbound RPC a <see cref="RaftReplica"/> fires. Unless a test configures a
/// canned response via <see cref="OnRequestVote"/>/<see cref="OnAppendEntries"/>, every call
/// throws — simulating an unreachable peer — so that layer-1 (message-injection) tests keep
/// full, exclusive control over which response messages ever reach the mailbox. A real or
/// simulated network with actual delivery semantics is layer 2's <c>SimulatedCluster</c>, not
/// this class.
/// </summary>
sealed class RecordingRaftTransport : IRaftTransport {
    public List<(string PeerId, VoteRequest Request)> VoteRequestsSent { get; } = [];
    public List<(string PeerId, AppendEntriesRequest Request)> AppendEntriesRequestsSent { get; } = [];

    public Func<string, VoteRequest, VoteResponse>? OnRequestVote { get; set; }
    public Func<string, AppendEntriesRequest, AppendEntriesResponse>? OnAppendEntries { get; set; }

    public ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken cancellationToken = default) {
        VoteRequestsSent.Add((peerId, request));

        if (OnRequestVote is null)
            throw new InvalidOperationException($"No canned RequestVote response configured for peer '{peerId}'.");

        return ValueTask.FromResult(OnRequestVote(peerId, request));
    }

    public ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        AppendEntriesRequestsSent.Add((peerId, request));

        if (OnAppendEntries is null)
            throw new InvalidOperationException($"No canned AppendEntries response configured for peer '{peerId}'.");

        return ValueTask.FromResult(OnAppendEntries(peerId, request));
    }

    public ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException("Snapshots are not covered by layer-1 tests yet.");
}
