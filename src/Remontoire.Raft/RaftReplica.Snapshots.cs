using Microsoft.Extensions.Logging;
using Remontoire.Raft.V1;
using Remontoire.Storage;

namespace Remontoire.Raft;

public sealed partial class RaftReplica {
    /// <inheritdoc cref="ReceiveVoteRequestAsync"/>
    public ValueTask<InstallSnapshotResponse> ReceiveInstallSnapshotAsync(InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        var reply = new TaskCompletionSource<InstallSnapshotResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new InstallSnapshotReceived(request, reply));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<InstallSnapshotResponse>(reply.Task.WaitAsync(cancellationToken));
    }

    // Receiving/serving InstallSnapshot itself is not implemented yet. Left as an explicit gap
    // rather than a silent no-op: a caller that invokes this before it's built will see the
    // failure surface (logged, reply never resolves) instead of a misleadingly "successful"
    // response. Self-triggered snapshot-taking (below) does not depend on this.
    Task HandleInstallSnapshotReceivedAsync(InstallSnapshotReceived message) =>
        throw new NotImplementedException("InstallSnapshot is not implemented yet.");

    Task HandleInstallSnapshotResponseReceivedAsync(InstallSnapshotResponseReceived message) =>
        throw new NotImplementedException("InstallSnapshot is not implemented yet.");

    // Called after every commit advance (both roles — a follower bounds its own WAL exactly like
    // a leader does). Cheap to call unconditionally: every guard below is an in-memory or
    // already-durable-log check, no I/O, until the point where a round trip actually starts.
    async Task TryTriggerSnapshotAsync() {
        if (prepareSnapshot is null || _snapshotInProgress)
            return;

        if (raftLog.LastIndex - raftLog.SnapshotIndex <= replicaConfig.SnapshotThresholdEntries)
            return;

        var lastIncludedIndex = _commitIndex;
        if (lastIncludedIndex <= raftLog.SnapshotIndex)
            return; // nothing newly committed since the last snapshot yet

        _snapshotInProgress = true;

        var lastIncludedTerm = await raftLog.GetTermAtAsync(lastIncludedIndex);

        // The LogicalOffset this snapshot's base sits at — same derivation as
        // RecoverNextLogicalOffsetAsync, bounded to this snapshot's range instead of the full tail.
        var nextLogicalOffset = _snapshotNextLogicalOffset;
        await foreach (var record in raftLog.ReadFromAsync(raftLog.SnapshotIndex + 1)) {
            if (record.RaftIndex > lastIncludedIndex)
                break;
            if (record.RecordType == WalRecordType.Append)
                nextLogicalOffset = record.LogicalOffset + 1;
        }

        var cancellationToken = _cts?.Token ?? CancellationToken.None;
        _ = Task.Run(async () => {
            try {
                // Ensures everything below nextLogicalOffset is durable in a segment — the
                // returned paths matter only to an InstallSnapshot serve, not to compaction
                // itself, so they're discarded here; a later serve re-queries fresh instead of
                // caching this result.
                await prepareSnapshot(nextLogicalOffset, cancellationToken);
                _channel.Writer.TryWrite(new SnapshotPrepared(lastIncludedIndex, lastIncludedTerm, nextLogicalOffset));
            } catch (Exception ex) {
                logger?.LogWarning(ex, "Snapshot preparation failed in {ShardGroupId}", GroupId);
                _channel.Writer.TryWrite(new SnapshotPreparationFailed());
            }
        }, cancellationToken);
    }

    // Persist before compact — a crash in between leaves SnapshotNextLogicalOffset ahead of
    // raftLog.SnapshotIndex, which is safe (RecoverNextLogicalOffsetAsync's scan still covers
    // the gap); the reverse order is not.
    async Task HandleSnapshotPreparedAsync(SnapshotPrepared message) {
        _snapshotInProgress = false;
        _snapshotNextLogicalOffset = message.NextLogicalOffset;
        await stateStore.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor, _snapshotNextLogicalOffset));
        await raftLog.CompactToAsync(message.LastIncludedIndex, message.LastIncludedTerm);
    }

    Task HandleSnapshotPreparationFailedAsync(SnapshotPreparationFailed message) {
        _snapshotInProgress = false; // a later commit advance gets to try again
        return Task.CompletedTask;
    }
}
