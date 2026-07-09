namespace Remontoire.Storage;

public sealed partial class ShardLog {
    /// <summary>
    /// Ensures every record below <paramref name="upToLogicalOffsetExclusive"/> is durable in a
    /// segment (forcing a flush if needed, regardless of <c>flushThresholdBytes</c>), then
    /// returns the current, complete segment file list. Waits if the actor hasn't caught up to
    /// <paramref name="upToLogicalOffsetExclusive"/> in the committed stream yet.
    /// </summary>
    public Task<IReadOnlyList<string>> PrepareSnapshotAsync(ulong upToLogicalOffsetExclusive, CancellationToken cancellationToken = default) {
        var completion = new TaskCompletionSource<IReadOnlyList<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _mailbox.Writer.TryWrite(new PrepareSnapshotRequested(upToLogicalOffsetExclusive, completion));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return completion.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Replaces the segments and MemTable wholesale with a snapshot received from elsewhere —
    /// any of this replica's own segments are discarded, conflicting or not, in favor of
    /// <paramref name="segmentPaths"/>.
    /// </summary>
    public Task InstallSnapshotAsync(IReadOnlyList<string> segmentPaths, ulong nextOffsetToApply, CancellationToken cancellationToken = default) {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _mailbox.Writer.TryWrite(new SnapshotInstalled(segmentPaths, nextOffsetToApply, completion));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return completion.Task.WaitAsync(cancellationToken);
    }

    async Task HandlePrepareSnapshotRequestedAsync(PrepareSnapshotRequested request) {
        _pendingSnapshotRequests.Add(request);
        await TryFulfillPendingSnapshotRequestsAsync();
    }

    // Called both when a new request arrives (it may already be satisfiable immediately) and
    // after every WAL-apply that advances _nextOffsetToApply (ShardLog.Apply.cs) — the only two
    // moments a previously-unsatisfiable request could become ready.
    async Task TryFulfillPendingSnapshotRequestsAsync() {
        if (_pendingSnapshotRequests.Count == 0)
            return;

        if (!_pendingSnapshotRequests.Exists(request => _nextOffsetToApply >= request.UpToLogicalOffsetExclusive))
            return; // still catching up — nothing ready yet

        if (_memTable.EstimatedSizeBytes > 0) {
            var (memTable, segments) = await FlushAsync(_directory, new ShardState(_memTable, _segments), CancellationToken.None);
            Volatile.Write(ref _segments, segments);
            Volatile.Write(ref _memTable, memTable);
        }

        var paths = Array.ConvertAll(_segments, segment => segment.Path);
        for (var i = _pendingSnapshotRequests.Count - 1; i >= 0; i--) {
            if (_nextOffsetToApply < _pendingSnapshotRequests[i].UpToLogicalOffsetExclusive)
                continue;

            _pendingSnapshotRequests[i].Completion.TrySetResult(paths);
            _pendingSnapshotRequests.RemoveAt(i);
        }
    }

    async Task HandleSnapshotInstalledAsync(SnapshotInstalled message) {
        var oldSegments = _segments;
        var newSegments = new SstSegment[message.SegmentPaths.Count];
        for (var i = 0; i < message.SegmentPaths.Count; i++)
            newSegments[i] = await SstSegment.OpenAsync(message.SegmentPaths[i]);

        Volatile.Write(ref _segments, newSegments);
        Volatile.Write(ref _memTable, new MemTable());
        _nextOffsetToApply = message.NextOffsetToApply;

        // Safe to delete immediately even if a concurrent TryGet/ReadFromAsync still holds one
        // of these open (FileShare.Delete — same precedent as CompactionPlan.MergeAsync), but
        // not to Dispose() them here: no safe release point without reference counting, same
        // tradeoff as the old MemTable after an ordinary flush.
        foreach (var oldSegment in oldSegments)
            File.Delete(oldSegment.Path);

        message.Completion.TrySetResult();
    }
}
