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
        if (committed.Record.RecordType != WalRecordType.Append)
            return;

        var (memTable, sstSegments) = await ApplyAndMaybeFlushAsync(_directory, new ShardState(_memTable, _segments), committed.Record, _flushThresholdBytes, CancellationToken.None);

        // Publish the NEW source of truth before retracting the old one — otherwise a
        // concurrent TryGet/ReadFromAsync could observe neither the (already-emptied) old
        // MemTable nor the (not-yet-published) new segment for an offset that legitimately
        // exists.
        Volatile.Write(ref _segments, sstSegments);
        Volatile.Write(ref _memTable, memTable);
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
