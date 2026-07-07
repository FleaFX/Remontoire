namespace Remontoire.Storage;

/// <summary>
/// Merges multiple small, adjacent SST segments in a directory into fewer, larger ones.
/// Phase 1: age-/size-driven only — no ack-driven pruning yet (needs
/// Remontoire.Messaging's ack-index, a later phase).
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

    static IEnumerable<List<SegmentCandidate>> GroupForMerge(List<SegmentCandidate> candidates, long? maxMergedSegmentBytes) {
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

    readonly record struct SegmentCandidate(string Path, SstSegment Segment);
}

/// <summary>
/// Phase-1 compaction policy: purely age-/size-driven. Ack-driven pruning is a later phase
/// (Remontoire.Messaging's ack-index does not exist yet).
/// </summary>
/// <param name="MaxAge">Segments written more recently than this are left alone. <c>null</c> considers every segment regardless of age.</param>
/// <param name="MaxMergedSegmentBytes">The target maximum size, in bytes, of a merged output segment — consecutive small segments are packed together up to this size. <c>null</c> means unlimited (pack everything into one segment).</param>
public sealed record CompactionPolicy(TimeSpan? MaxAge, long? MaxMergedSegmentBytes);
