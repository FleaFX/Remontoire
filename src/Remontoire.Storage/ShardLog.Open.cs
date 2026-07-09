namespace Remontoire.Storage;

public sealed partial class ShardLog {
    /// <summary>
    /// Opens (creating if necessary) the shard log in <paramref name="directory"/>, consuming
    /// committed records from <paramref name="committedSource"/> from wherever it currently
    /// stands. Recovery here is limited to loading existing SST segments; replaying the durable
    /// log tail is the committed-source's own owner's responsibility, not <see cref="ShardLog"/>'s.
    /// </summary>
    public static async Task<ShardLog> OpenAsync(
        string directory, Func<CancellationToken, IAsyncEnumerable<WalRecord>> committedSource,
        long flushThresholdBytes = 64 * 1024 * 1024, CompactionPolicy? compactionPolicy = null, CancellationToken cancellationToken = default) {
        RecoverInterruptedCompactions(directory);

        var segments = await LoadSegmentsAsync(directory, cancellationToken);
        var nextOffsetToApply = segments.Length > 0 ? segments[^1].MaxOffset + 1 : 0UL;

        return new ShardLog(directory, committedSource, new MemTable(), segments, nextOffsetToApply, flushThresholdBytes, compactionPolicy);
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
}
