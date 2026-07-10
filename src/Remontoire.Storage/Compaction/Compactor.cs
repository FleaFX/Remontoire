namespace Remontoire.Storage;

/// <summary>
/// Merges multiple small, adjacent SST segments in a directory into fewer, larger ones. Also
/// runs ack-driven pruning (<see cref="PruneAckedSegmentsAsync"/>) when the policy carries a
/// watermark delegate — that operation drops fully-acked segments outright rather than merging.
/// </summary>
static class Compactor {
    /// <summary>
    /// Runs one compaction pass over the SST segments in <paramref name="directory"/>.
    /// </summary>
    public static async Task RunAsync(string directory, CompactionPolicy policy, CancellationToken cancellationToken = default) {
        var candidates = await LoadCandidatesAsync(directory, policy, cancellationToken);

        foreach (var group in GroupForMerge(candidates, policy.MaxMergedSegmentBytes)) {
            if (group.Count < 2) {
                group[0].Segment.Dispose(); // nothing to merge — release the handle we opened to inspect it
                continue;
            }

            await MergeGroupAsync(directory, group, cancellationToken);
        }
    }

    static async Task<List<SegmentCandidate>> LoadCandidatesAsync(string directory, CompactionPolicy policy, CancellationToken cancellationToken) {
        var candidates = new List<SegmentCandidate>();

        foreach (var path in Directory.EnumerateFiles(directory, "*.sst")) {
            var age = DateTime.UtcNow - File.GetLastWriteTimeUtc(path);
            if (policy.MaxAge is { } maxAge && age < maxAge)
                continue; // too recent to bother compacting yet

            var segment = await SstSegment.OpenAsync(path, cancellationToken);
            candidates.Add(new SegmentCandidate(path, segment));
        }

        candidates.Sort((a, b) => a.Segment.MinOffset.CompareTo(b.Segment.MinOffset));
        return candidates;
    }

    public static IEnumerable<List<SegmentCandidate>> GroupForMerge(List<SegmentCandidate> candidates, long? maxMergedSegmentBytes) {
        var group = new List<SegmentCandidate>();
        var groupBytes = 0L;

        foreach (var candidate in candidates) {
            var size = new FileInfo(candidate.Path).Length;

            if (group.Count > 0 && maxMergedSegmentBytes is { } max && groupBytes + size > max) {
                yield return group;
                group = [];
                groupBytes = 0;
            }

            group.Add(candidate);
            groupBytes += size;
        }

        if (group.Count > 0)
            yield return group;
    }

    static async Task MergeGroupAsync(string directory, List<SegmentCandidate> group, CancellationToken cancellationToken) {
        var finalPath = Path.Combine(directory, $"segment-{group[0].Segment.MinOffset:D20}.sst");
        var mergedPath = finalPath + ".merging";

        await SstWriter.WriteAsync(mergedPath, MergedEntries(group), cancellationToken: cancellationToken);

        foreach (var candidate in group) {
            candidate.Segment.Dispose();
            File.Delete(candidate.Path);
        }

        File.Move(mergedPath, finalPath);
    }

    static IEnumerable<LogEntry> MergedEntries(List<SegmentCandidate> group) {
        foreach (var candidate in group)
            foreach (var result in candidate.Segment.ScanFrom(candidate.Segment.MinOffset))
                using (result)
                    yield return result.Entry;
    }

    public readonly record struct SegmentCandidate(string Path, SstSegment Segment);

    /// <summary>
    /// Drops every segment whose entire offset range is already covered by every consumer
    /// group's low watermark — the first of the retention conditions a message must satisfy
    /// before it may ever be discarded; the others (audit/max-age windows, a size-driven
    /// emergency floor) are a later phase's concern. A no-op when <paramref name="policy"/>
    /// carries no <see cref="CompactionPolicy.GetAckedLowWatermarkAsync"/> delegate — the
    /// age-/size-driven <see cref="RunAsync"/> pass (unchanged) is the only pruning that happens
    /// for a policy without one.
    /// </summary>
    public static async Task PruneAckedSegmentsAsync(string directory, CompactionPolicy policy, CancellationToken cancellationToken = default) {
        if (policy.GetAckedLowWatermarkAsync is not { } getWatermark)
            return;

        // Exclusive: the delegate's contract is "every offset below this is acked" — a segment
        // is only fully covered when even its own MaxOffset falls below it, not at or below.
        var watermark = await getWatermark(cancellationToken);

        foreach (var path in Directory.EnumerateFiles(directory, "*.sst")) {
            using var segment = await SstSegment.OpenAsync(path, cancellationToken);
            if (segment.MaxOffset < watermark)
                File.Delete(path); // whole, fully-acked segment — no partial rewrite needed
        }
    }
}

/// <summary>
/// Phase-1 compaction policy: age-/size-driven merging, plus an optional ack-driven pruning
/// hook.
/// </summary>
/// <param name="MaxAge">Segments written more recently than this are left alone. <c>null</c> considers every segment regardless of age.</param>
/// <param name="MaxMergedSegmentBytes">The target maximum size, in bytes, of a merged output segment — consecutive small segments are packed together up to this size. <c>null</c> means unlimited (pack everything into one segment).</param>
/// <param name="GetAckedLowWatermarkAsync">
/// Optional injection point: returns the lowest low-watermark across every registered consumer
/// group on this shard — exclusive (every offset below the returned value is acked by every
/// group; zero means no group has acked anything yet) — so <see cref="Compactor.PruneAckedSegmentsAsync"/>
/// can drop segments no group can still read from. This project never references the ack-tracking
/// layer directly — same one-way dependency discipline as everywhere else a lower layer needs a
/// fact only a higher one can supply. <c>null</c> disables ack-driven pruning entirely — nothing
/// is ever dropped on age/size grounds alone.
/// </param>
public sealed record CompactionPolicy(TimeSpan? MaxAge, long? MaxMergedSegmentBytes, Func<CancellationToken, ValueTask<ulong>>? GetAckedLowWatermarkAsync = null);
