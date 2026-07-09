namespace Remontoire.Storage;

public sealed partial class ShardLog {
    /// <summary>
    /// Opens (creating if necessary) the shard log in <paramref name="directory"/>. Recovery
    /// replays the WAL from the start, skipping whatever is already covered by existing SST
    /// segments, and applies the rest to a fresh MemTable.
    /// </summary>
    public static async Task<ShardLog> OpenAsync(
        string directory, long flushThresholdBytes = 64 * 1024 * 1024, CompactionPolicy? compactionPolicy = null, CancellationToken cancellationToken = default) {
        RecoverInterruptedCompactions(directory);

        var segments = await LoadSegmentsAsync(directory, cancellationToken);
        var flushedWatermark = segments.Length > 0 ? segments[^1].MaxOffset + 1 : 0UL;

        var walPath = EnsureWalFileExists(Path.Combine(directory, "wal.log"));
        var (state, nextLogicalOffset) = await ReplayWalAsync(
            directory, new WalReader(walPath), new ShardState(new MemTable(), segments), flushedWatermark, flushThresholdBytes, cancellationToken);

        var walWriter = await WalWriter.OpenAsync(walPath, cancellationToken);
        return new ShardLog(directory, walWriter, state.MemTable, state.Segments, nextLogicalOffset, flushThresholdBytes, compactionPolicy);
    }

    // A crash between a compaction's "delete old inputs" and "rename to final name" steps
    // leaves a fully durable, just-not-yet-visible `*.sst.merging` file behind — safe to rename
    // blindly, the files it would have replaced are already gone.
    static void RecoverInterruptedCompactions(string directory) {
        foreach (var merging in Directory.EnumerateFiles(directory, "*.sst.merging"))
            File.Move(merging, merging[..^".merging".Length]);
    }

    static async Task<SstSegment[]> LoadSegmentsAsync(string directory, CancellationToken cancellationToken) {
        var segments = new List<SstSegment>();
        foreach (var path in Directory.EnumerateFiles(directory, "*.sst"))
            segments.Add(await SstSegment.OpenAsync(path, cancellationToken));

        segments.Sort((a, b) => a.MinOffset.CompareTo(b.MinOffset));
        return segments.ToArray();
    }

    static string EnsureWalFileExists(string walPath) {
        if (!File.Exists(walPath))
            File.Create(walPath).Dispose();
        return walPath;
    }

    // Reads the WAL from byte 0, skipping anything already covered by an existing segment
    // (LogicalOffset < flushedWatermark), applying the rest. This is a one-shot read — once
    // ShardLog is live, new appends never round-trip back through the WAL file at all; they
    // flow directly from WalWriter's own in-memory ReadDurableAsync stream instead.
    static async Task<(ShardState State, ulong NextLogicalOffset)> ReplayWalAsync(
        string directory, WalReader walReader, ShardState state, ulong flushedWatermark, long flushThresholdBytes, CancellationToken cancellationToken) {
        var nextLogicalOffset = flushedWatermark;

        await foreach (var result in walReader.ReadFromAsync(0, cancellationToken)) {
            using (result) {
                if (result.Record.RecordType == WalRecordType.Append && result.Record.LogicalOffset >= flushedWatermark) {
                    state = await ApplyAndMaybeFlushAsync(directory, state, result.Record, flushThresholdBytes, cancellationToken);
                    nextLogicalOffset = result.Record.LogicalOffset + 1;
                }
            }
        }

        return (state, nextLogicalOffset);
    }
}
