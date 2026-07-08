namespace Remontoire.Storage.Compaction;

/// <summary>
/// The exact set of segments a compaction merge was planned against, fixed at plan time — only
/// these get replaced once the merge completes, never "whatever's currently there", so a flush
/// that happens to land while the merge is running is left alone.
/// </summary>
readonly record struct CompactionPlan(SstSegment[] Sources) {
    /// <summary>
    /// Executes the merge: writes <see cref="Sources"/> into one new segment file via the usual
    /// temp+fsync+rename discipline, then deletes the old files. Returns the final segment path.
    /// Does not touch the actor's live segment array — that happens afterward, via
    /// <see cref="ReplaceIn"/>, once the actor has processed the result.
    /// </summary>
    public async Task<string> MergeAsync() {
        var directory = Path.GetDirectoryName(Sources[0].Path)!;
        var finalPath = Path.Combine(directory, $"segment-{Sources[0].MinOffset:D20}.sst");
        var mergedPath = finalPath + ".merging";

        await SstWriter.WriteAsync(mergedPath, MergedEntries());

        foreach (var source in Sources)
            File.Delete(source.Path); // safe thanks to FileShare.Delete, even if a live SstSegment still has it open

        File.Move(mergedPath, finalPath);
        return finalPath;
    }

    /// <summary>
    /// Given the actor's current segment array and the freshly opened merged segment, returns
    /// the array with exactly this plan's <see cref="Sources"/> replaced by
    /// <paramref name="merged"/> — never "whatever's currently there", so a segment a concurrent
    /// flush added while this merge was running survives untouched.
    /// </summary>
    public SstSegment[] ReplaceIn(SstSegment[] segments, SstSegment merged) =>
        segments.Except(Sources).Append(merged).OrderBy(s => s.MinOffset).ToArray();

    IEnumerable<LogEntry> MergedEntries() {
        foreach (var source in Sources)
            foreach (var result in source.ScanFrom(source.MinOffset))
                using (result)
                    yield return result.Entry;
    }
}
