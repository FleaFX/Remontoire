using System.Threading.Channels;
using Remontoire.Storage;

namespace Remontoire.Raft;

public sealed partial class RaftReplica {
    readonly Channel<RaftReplicaMessage> _channel = Channel.CreateUnbounded<RaftReplicaMessage>(new UnboundedChannelOptions { SingleReader = true });

    // The committed-record stream backing ReadCommittedAsync — the same handoff pattern as
    // WalWriter's _committed channel in phase 1, one level up: this one carries quorum-committed
    // records, not merely fsynced ones.
    readonly Channel<WalRecord> _committed = Channel.CreateUnbounded<WalRecord>(new UnboundedChannelOptions { SingleReader = true });

    Task? _actorLoopTask;
    CancellationTokenSource? _cts;

    // Persistent Raft state — mirrored here after every SaveAsync; the store is the truth.
    ulong _currentTerm;
    string? _votedFor;

    // Externally visible actor state: written only by the loop (via Volatile.Write), read via
    // the public properties from any thread.
    ReplicaRole _role;
    ulong _commitIndex;
    string? _leaderHint;
    bool _isLeaderReady;

    // Candidate state — reset on every transition into Candidate. One RPC per peer per election
    // means no duplicate responses, so a plain count (self-vote included) suffices.
    int _votesReceived;

    // Leader state — initialized on every transition into Leader, nulled on step-down.
    Dictionary<string, ulong>? _nextIndex;
    Dictionary<string, ulong>? _matchIndex;
    SortedDictionary<ulong, PendingProposal>? _pendingProposals;
    ulong _nextLogicalOffset;
    ulong _leaderNoOpIndex;

    // Mirrors RaftPersistentState.SnapshotNextLogicalOffset after every SaveAsync — the
    // nextLogicalOffset as of the log's own compacted base, zero when none was ever taken.
    // Floors RecoverNextLogicalOffsetAsync's scan for the compacted log prefix.
    ulong _snapshotNextLogicalOffset;

    // Non-null exactly while a prepareSnapshot round trip is in flight (background task, not yet
    // replied) — set by TryTriggerSnapshotAsync, cleared by HandleSnapshotPreparedAsync/
    // HandleSnapshotPreparationFailedAsync. Guards against firing a second round trip for every
    // commit advance while one is already outstanding.
    bool _snapshotInProgress;

    /// <summary>
    /// A proposal awaiting quorum commit: the result it will resolve to (RaftIndex and
    /// LogicalOffset are assigned at propose time) and the caller's completion.
    /// </summary>
    readonly record struct PendingProposal(ProposeResult Result, TaskCompletionSource<ProposeResult> Reply);

    /// <summary>
    /// The number of replicas (self included) that constitutes a majority of the group.
    /// </summary>
    int QuorumSize => (replicaConfig.Peers.Count + 1) / 2 + 1;

    // Test-only seams: internal, reached via InternalsVisibleTo (testprojecten-only convention).
    // Tests inject messages directly rather than driving them through a real or simulated
    // network, and assert on state that has no public equivalent.

    /// <summary>
    /// The replica's current role. No public equivalent — <see cref="RaftReplica.IsLeader"/>
    /// is the public-facing signal; this exists for tests that need to distinguish
    /// Follower/Candidate directly.
    /// </summary>
    internal ReplicaRole Role => _role;

    /// <summary>
    /// The candidate this replica voted for in the current term, or <see langword="null"/> if
    /// it hasn't voted yet.
    /// </summary>
    internal string? VotedFor => _votedFor;

    /// <summary>
    /// Posts <paramref name="message"/> directly to the actor's mailbox — the test-only
    /// equivalent of a timer firing or an RPC arriving.
    /// </summary>
    internal bool TryPost(RaftReplicaMessage message) => _channel.Writer.TryWrite(message);

    /// <summary>
    /// Posts a <see cref="DrainSentinel"/> and awaits it: every message posted before this call
    /// is guaranteed processed once this returns — no <c>Task.Delay</c> polling, no timing.
    /// </summary>
    internal async Task DrainAsync(CancellationToken cancellationToken = default) {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        TryPost(new DrainSentinel(completion));
        await completion.Task.WaitAsync(cancellationToken);
    }

    static Task HandleDrainSentinel(DrainSentinel sentinel) {
        sentinel.Completion.TrySetResult();
        return Task.CompletedTask;
    }

    /// <summary>
    /// The one transition every "saw a higher term" rule funnels into. Persists the term bump
    /// before returning, so no caller can resolve an RPC reply before the durable-before-respond
    /// rule is satisfied.
    /// </summary>
    async Task BecomeFollowerAsync(ulong term, string? leaderHint) {
        if (term > _currentTerm) {
            Volatile.Write(ref _currentTerm, term);
            _votedFor = null;
            await stateStore.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor, _snapshotNextLogicalOffset));
        }

        _role = ReplicaRole.Follower;
        Volatile.Write(ref _isLeaderReady, false);
        Volatile.Write(ref _leaderHint, leaderHint);
        _votesReceived = 0;
        _nextIndex = null;
        _matchIndex = null;
        _heartbeatTimerGeneration++; // invalidate any scheduled heartbeat tick without re-arming

        // A demoted leader can no longer decide commits. The entries behind these proposals may
        // still commit under the new leader, but this replica can no longer truthfully promise
        // that — fail them with the redirect hint and let phase 4's protocol drive the retry.
        FailPendingProposals();

        // Liveness invariant: a non-leader always has an armed election timer.
        RestartElectionTimer();
    }

    /// <summary>
    /// Fails all pending proposals with <see cref="NotLeaderException"/>. Safe to call when
    /// there are no pending proposals (no-op).
    /// </summary>
    void FailPendingProposals() {
        if (_pendingProposals is null)
            return;

        foreach (var pending in _pendingProposals.Values)
            pending.Reply.TrySetException(new NotLeaderException(GroupId, _leaderHint));

        _pendingProposals = null;
    }
}
