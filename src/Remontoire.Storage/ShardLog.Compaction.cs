using Remontoire.Storage.Compaction;

namespace Remontoire.Storage;

public sealed partial class ShardLog {
    void HandleCompactionPlanRequest(CompactionPlanRequest request) {
        var plan = TryComputePlan();
        if (plan is { } ready)
            request.Response.TrySetResult(ready);
        else
            _pendingPlanRequest = request.Response; // fulfilled later by TryFulfillPendingPlanRequest
    }

    // Called after every successful flush — a new segment can be exactly what turns an earlier
    // "nothing to do" answer into a valid plan.
    void TryFulfillPendingPlanRequest() {
        if (_pendingPlanRequest is not { } pending)
            return;

        var plan = TryComputePlan();
        if (plan is not { } ready)
            return;

        pending.TrySetResult(ready);
        _pendingPlanRequest = null;
    }

    CompactionPlan? TryComputePlan() {
        if (_compactionPolicy is not { } policy || _segments.Length < 2)
            return null;

        var candidates = new List<Compactor.SegmentCandidate>();
        foreach (var segment in _segments) {
            var age = DateTime.UtcNow - File.GetLastWriteTimeUtc(segment.Path);
            if (policy.MaxAge is not { } maxAge || age >= maxAge)
                candidates.Add(new Compactor.SegmentCandidate(segment.Path, segment));
        }

        foreach (var group in Compactor.GroupForMerge(candidates, policy.MaxMergedSegmentBytes)) {
            if (group.Count >= 2)
                return new CompactionPlan([.. group.Select(c => c.Segment)]);
        }

        return null;
    }

    void HandleCompactionCompleted(CompactionCompleted completed) {
        if (completed.MergedPath is not { } mergedPath)
            return; // the merge itself failed — nothing to change on _segments

        // A concurrent ack-/size-driven prune may have already removed one of this plan's own
        // source segments from _segments (and from disk) while the merge was still running
        // against its old, still-open handles (FileShare.Delete lets that read succeed regardless).
        // Applying the merge result in that case would resurrect the just-pruned data — both back
        // into _segments and back onto disk (this merge's own File.Move recreates it). Discard
        // instead: delete the now-orphaned merged file and leave _segments untouched — whatever's
        // still actually there keeps its true, current state; the next compaction pass simply
        // retries with whatever's still around.
        if (completed.Plan.Sources.Any(source => Array.IndexOf(_segments, source) < 0)) {
            File.Delete(mergedPath);
            return;
        }

        var merged = SstSegment.Open(mergedPath);
        Volatile.Write(ref _segments, completed.Plan.ReplaceIn(_segments, merged));
        // No Dispose() of completed.Plan.Sources — same tradeoff as the old MemTable after a
        // flush: no safe release point without reference counting (a concurrent
        // TryGet/ReadFromAsync call might still hold a reference). SafeFileHandle's own
        // finalizer eventually reclaims the OS handle — acceptable since compaction runs
        // infrequently and only ever replaces a handful of segments at a time.
    }
}
