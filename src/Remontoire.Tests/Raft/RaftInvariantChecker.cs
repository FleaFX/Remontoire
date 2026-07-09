namespace Remontoire.Raft;

/// <summary>
/// Watches every replica in a <see cref="SimulatedCluster"/> and, on every call to
/// <see cref="AssertAsync"/>, machine-checks Raft's safety properties across the whole run so
/// far — not just the current step. A violation throws <see cref="RaftInvariantViolationException"/>
/// immediately, turning a divergence into a test failure at the exact step it happened, instead of
/// a much-later, much-harder-to-explain symptom.
/// </summary>
/// <remarks>
/// Covers four of the five properties by polling externally-observable state (role, term,
/// commit index, log content) after each simulation step — deliberately not the fifth, Leader
/// Append-Only: that one is a constraint on which *code path* is allowed to call
/// <c>IRaftLog.TruncateFromAsync</c> (the follower-receiving path only, never the leader-sending
/// path), which isn't something an external observer can poll for. It is verified by construction
/// (code review), not at runtime here.
///
/// Only the already-committed prefix of each replica's log is compared across replicas — entries
/// above a replica's own commit index are expected to legitimately diverge and get resolved by
/// the normal conflict-detection path (that's business as usual, not a safety violation); checking
/// them here would just produce false positives on every ordinary in-flight replication.
/// </remarks>
sealed class RaftInvariantChecker(SimulatedCluster cluster) {
    // Election Safety: at most one leader per term, ever, across the whole run.
    readonly Dictionary<ulong, string> _leaderByTerm = [];

    // The term already observed, on any replica, at each index once that index was part of
    // *that* replica's committed prefix. Once recorded, every other replica's committed prefix —
    // and every current leader's full log — must agree with it forever after.
    readonly SortedDictionary<ulong, ulong> _committedTermByIndex = [];

    /// <summary>
    /// Runs every check once against the cluster's current state. Safe to call after every
    /// <see cref="SimulatedCluster.StepAsync"/> — cheap (bounded by how far commit indexes have
    /// advanced since the last call, not by total run length).
    /// </summary>
    public async Task AssertAsync() {
        await CheckElectionSafetyAsync();
        await CheckCommittedPrefixesAgreeAsync();
        await CheckLeaderCompletenessAsync();
    }

    async Task CheckElectionSafetyAsync() {
        foreach (var (nodeId, replica) in cluster.Replicas) {
            if (!replica.IsLeader)
                continue;

            var term = replica.CurrentTerm;
            if (_leaderByTerm.TryGetValue(term, out var existingLeader)) {
                if (existingLeader != nodeId)
                    throw new RaftInvariantViolationException(
                        $"Election Safety violated: both '{existingLeader}' and '{nodeId}' claim leadership in term {term}.{await DumpAsync()}");
            } else {
                _leaderByTerm[term] = nodeId;
            }
        }
    }

    // Also covers Log Matching and State Machine Safety for the committed portion of the log —
    // the only portion where cross-replica agreement is actually mandatory at any given instant.
    async Task CheckCommittedPrefixesAgreeAsync() {
        foreach (var (nodeId, replica) in cluster.Replicas) {
            if (!cluster.Logs.TryGetValue(nodeId, out var log))
                continue;

            var commitIndex = Math.Min(replica.CommitIndex, log.LastIndex);
            for (var index = log.SnapshotIndex + 1; index <= commitIndex; index++) {
                var term = await log.GetTermAtAsync(index);

                if (_committedTermByIndex.TryGetValue(index, out var knownTerm)) {
                    if (knownTerm != term)
                        throw new RaftInvariantViolationException(
                            $"Log Matching / State Machine Safety violated at index {index}: '{nodeId}' has committed term {term} there, but term {knownTerm} was already committed there by another replica.{await DumpAsync()}");
                } else {
                    _committedTermByIndex[index] = term;
                }
            }
        }
    }

    // A committed entry must be present, unchanged, in the log of every leader of a LATER term —
    // checked against every index ever recorded as committed by ANY replica so far, not just
    // this one's. A currently-observed "leader" whose own term predates the entry's committed
    // term is a stale believer that simply hasn't heard about the newer term yet (normal,
    // harmless Raft behavior on its own — it can't commit anything new while stale) — exempt, not
    // a violation: the property is about later terms, not about every leader ever seen.
    async Task CheckLeaderCompletenessAsync() {
        foreach (var (nodeId, replica) in cluster.Replicas) {
            if (!replica.IsLeader || !cluster.Logs.TryGetValue(nodeId, out var log))
                continue;

            foreach (var (index, expectedTerm) in _committedTermByIndex) {
                if (expectedTerm > replica.CurrentTerm)
                    continue; // this leader's term predates the commit — nothing to require yet

                if (index <= log.SnapshotIndex)
                    continue; // compacted away — snapshotting isn't implemented yet, so unreachable today; kept for when it is.

                if (index > log.LastIndex)
                    throw new RaftInvariantViolationException(
                        $"Leader Completeness violated: leader '{nodeId}' (term {replica.CurrentTerm}) is missing committed index {index} — its log only reaches {log.LastIndex}.{await DumpAsync()}");

                var actualTerm = await log.GetTermAtAsync(index);
                if (actualTerm != expectedTerm)
                    throw new RaftInvariantViolationException(
                        $"Leader Completeness violated: leader '{nodeId}' has term {actualTerm} at index {index}, but term {expectedTerm} was already committed there.{await DumpAsync()}");
            }
        }
    }

    // A full snapshot of every replica's externally-observable state, appended to every
    // violation message — the whole point is to never have to reproduce a timing-dependent
    // failure just to see what the cluster looked like when it happened.
    async Task<string> DumpAsync() {
        var lines = new List<string> { "", "--- cluster state at the moment of violation ---" };

        foreach (var (nodeId, log) in cluster.Logs) {
            var running = cluster.Replicas.TryGetValue(nodeId, out var replica);
            var status = running
                ? $"role={replica!.Role}, term={replica.CurrentTerm}, commitIndex={replica.CommitIndex}, votedFor={replica.VotedFor ?? "null"}"
                : "CRASHED";
            var tail = await DumpLogTailAsync(log);
            lines.Add($"  {nodeId}: {status}, log=[snapshotIndex={log.SnapshotIndex}, lastIndex={log.LastIndex}, tailTerms={tail}]");
        }

        lines.Add($"  known-committed (index -> term): [{string.Join(", ", _committedTermByIndex.Select(pair => $"{pair.Key}->{pair.Value}"))}]");
        return string.Join(Environment.NewLine, lines);
    }

    static async Task<string> DumpLogTailAsync(IRaftLog log) {
        var terms = new List<string>();
        for (var index = log.SnapshotIndex + 1; index <= log.LastIndex; index++)
            terms.Add($"{index}:{await log.GetTermAtAsync(index)}");
        return $"[{string.Join(", ", terms)}]";
    }
}
