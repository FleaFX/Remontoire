namespace Remontoire.Storage;

public sealed partial class ShardLog {
    static readonly TimeSpan RetentionTickInterval = TimeSpan.FromHours(1);

    async Task RunRetentionTickerAsync(CancellationToken cancellationToken) {
        try {
            while (true) {
                await Task.Delay(RetentionTickInterval, cancellationToken);
                _mailbox.Writer.TryWrite(new RetentionPassRequested());
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
        }
    }

    // Ack-/audit-gedreven pruning's actor-safe entry point — operates directly on the already-open
    // _segments instead of Compactor.PruneAckedSegmentsAsync's directory rescan, so _segments
    // itself always stays in sync with what's actually still on disk: no dead entry ever left
    // behind pointing at a file that's already gone.
    //
    // Deliberately does NOT Dispose() the removed SstSegment instances, even though they're no
    // longer reachable through _segments after this — same tradeoff HandleCompactionCompleted
    // already accepts for its own replaced segments (see that method's own comment): a concurrent
    // TryGet/ReadFromAsync call may have captured the OLD _segments array (or one of its entries)
    // just before this swap, and disposing out from under it would throw ObjectDisposedException
    // on that caller instead of a clean read. FileShare.Delete (SstSegment.cs) makes the File.Delete
    // itself safe regardless; the segment's own handle, and the OS disk space behind it, is
    // reclaimed once nothing references it anymore and it's eventually finalized.
    async Task HandleRetentionPassRequestedAsync() {
        if (_retentionPolicy?.IsAdmissionPaused?.Invoke() ?? false)
            return; // mid-reshard-pause — same discipline every new periodic loop below follows

        if (_compactionPolicy?.GetAckedLowWatermarkAsync is not { } getWatermark)
            return;

        var watermark = await getWatermark(CancellationToken.None);
        var remaining = new List<SstSegment>();

        foreach (var segment in _segments) {
            if (segment.MaxOffset < watermark)
                File.Delete(segment.Path);
            else
                remaining.Add(segment);
        }

        Volatile.Write(ref _segments, remaining.ToArray());
    }

    // SizePruneWorker already deleted these files itself, off the actor thread, before posting
    // this message — this only drops the matching entries from _segments, same no-Dispose()
    // discipline as HandleRetentionPassRequestedAsync above and for the identical reason. A path
    // with no matching live segment (already replaced by a concurrent compaction) is simply not
    // found — a harmless no-op.
    void HandleSizePruneCompleted(SizePruneCompleted completed) {
        var deletedPaths = new HashSet<string>(completed.DeletedPaths);
        Volatile.Write(ref _segments, _segments.Where(s => !deletedPaths.Contains(s.Path)).ToArray());
    }
}
