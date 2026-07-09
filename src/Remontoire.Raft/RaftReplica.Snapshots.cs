using Remontoire.Raft.V1;

namespace Remontoire.Raft;

public sealed partial class RaftReplica {
    /// <inheritdoc cref="ReceiveVoteRequestAsync"/>
    public ValueTask<InstallSnapshotResponse> ReceiveInstallSnapshotAsync(InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        var reply = new TaskCompletionSource<InstallSnapshotResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new InstallSnapshotReceived(request, reply));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<InstallSnapshotResponse>(reply.Task.WaitAsync(cancellationToken));
    }

    // Snapshots + InstallSnapshot are not implemented yet. Left as an explicit gap rather than a
    // silent no-op: a caller that invokes this before it's built will see the failure surface
    // (logged, reply never resolves) instead of a misleadingly "successful" response.
    Task HandleInstallSnapshotReceivedAsync(InstallSnapshotReceived message) =>
        throw new NotImplementedException("InstallSnapshot is not implemented yet.");

    Task HandleInstallSnapshotResponseReceivedAsync(InstallSnapshotResponseReceived message) =>
        throw new NotImplementedException("InstallSnapshot is not implemented yet.");
}
