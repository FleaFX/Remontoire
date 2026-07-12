namespace Remontoire.Storage;

public sealed partial class ShardLog {
    /// <summary>
    /// The MemTable/segments pair, always moved and replaced together — a flush swaps both at
    /// once (a new, empty MemTable alongside the grown segment list).
    /// </summary>
    readonly record struct ShardState(MemTable MemTable, SstSegment[] Segments);

    // Runs on the actor loop's single thread — the only place _memTable/_segments are ever
    // written, so reading them back here needs no Volatile.Read (no other writer exists to
    // race with). Volatile.Write below is still required: TryGet/ReadFromAsync read these
    // fields from other threads concurrently.
    async Task HandleWalRecordCommittedAsync(WalRecordCommitted committed) {
        // The committed-source may replay its full history from the start on every restart —
        // this layer doesn't know or care why, only that it must tolerate it. Anything at or
        // below the watermark already reached a segment or the MemTable in an earlier session;
        // re-applying it here would flush it a second time. Filtering purely by LogicalOffset
        // keeps this a plain, idempotent skip — meaningless for non-Append record types, so those
        // always fall through to the applied-republish below untouched.
        if (committed.Record.RecordType == WalRecordType.Append && committed.Record.LogicalOffset >= _nextOffsetToApply) {
            var before = _segments;
            var (memTable, sstSegments) = await ApplyAndMaybeFlushAsync(_directory, new ShardState(_memTable, before), committed.Record, _flushThresholdBytes, CancellationToken.None);
            Volatile.Write(ref _nextOffsetToApply, committed.Record.LogicalOffset + 1);

            // Publish the NEW source of truth before retracting the old one — otherwise a
            // concurrent TryGet/ReadFromAsync could observe neither the (already-emptied) old
            // MemTable nor the (not-yet-published) new segment for an offset that legitimately
            // exists.
            Volatile.Write(ref _segments, sstSegments);
            Volatile.Write(ref _memTable, memTable);

            // A new segment can be exactly what turns an earlier "nothing to compact" answer into
            // a valid plan — only worth checking when a flush actually grew the segment list.
            if (sstSegments.Length != before.Length)
                TryFulfillPendingPlanRequest();

            // A pending PrepareSnapshotAsync call may have been waiting on exactly this offset.
            await TryFulfillPendingSnapshotRequestsAsync();

            // Wake every WaitForAppendAsync waiter — swap in a fresh TaskCompletionSource BEFORE
            // completing the old one, so a waiter that immediately re-awaits after waking can
            // never land on an instance that's already completed and about to be discarded.
            var previouslyAppended = _appended;
            _appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            previouslyAppended.TrySetResult();
        }

        // Every record this shard log ever receives, applied or not (a replayed duplicate Append
        // included) — the single feed for anything downstream that needs the raw committed
        // stream. Unlike the MemTable/SST apply above, this has no idempotency filter of its own;
        // that's each consumer's own responsibility (e.g. an Ack-applier's naturally idempotent
        // Ack semantics).
        _applied.Writer.TryWrite(committed.Record);
    }

    /// <summary>
    /// Applies <paramref name="record"/> (assumed <see cref="WalRecordType.Append"/> — the
    /// caller filters, both recovery and <see cref="HandleWalRecordCommittedAsync"/> only ever
    /// call this for those) to <paramref name="state"/>'s MemTable, then flushes to a new SST
    /// segment if that pushed it over <paramref name="flushThresholdBytes"/>. Shared by recovery
    /// (<see cref="OpenAsync"/>) and live-apply so the two can never apply records differently.
    /// </summary>
    static async Task<ShardState> ApplyAndMaybeFlushAsync(
        string directory, ShardState state, WalRecord record, long flushThresholdBytes, CancellationToken cancellationToken) {
        state.MemTable.Append(new LogEntry(record.LogicalOffset, record.TimestampMicros, record.PartitionKey, record.Headers, record.Payload));

        return state.MemTable.EstimatedSizeBytes < flushThresholdBytes
            ? state
            : await FlushAsync(directory, state, cancellationToken);
    }

    static async Task<ShardState> FlushAsync(string directory, ShardState state, CancellationToken cancellationToken) {
        var path = Path.Combine(directory, $"segment-{state.MemTable.FirstOffset:D20}.sst");
        await SstWriter.WriteAsync(path, state.MemTable.ScanFrom(0), cancellationToken: cancellationToken);
        var newSegment = await SstSegment.OpenAsync(path, cancellationToken);

        var grownSegments = new SstSegment[state.Segments.Length + 1];
        Array.Copy(state.Segments, grownSegments, state.Segments.Length);
        grownSegments[^1] = newSegment;

        return new ShardState(new MemTable(), grownSegments);
    }
}
