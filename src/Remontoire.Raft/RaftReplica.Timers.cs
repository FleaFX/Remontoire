namespace Remontoire.Raft;

public sealed partial class RaftReplica {
    readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    readonly Random _random = replicaConfig.ElectionRandomSeed is { } seed ? new Random(seed) : Random.Shared;

    // timer generations — incremented on every (re)start; stale timer messages carry an old generation and are discarded.
    ulong _electionTimerGeneration;
    ulong _heartbeatTimerGeneration;

    // Test-only seams — let a test stamp a synthetic timer message with the actually-current
    // generation instead of guessing it.

    /// <summary>
    /// The election timer's current generation. A test uses this to construct a deliberately
    /// stale <see cref="ElectionTimeoutElapsed"/> message.
    /// </summary>
    internal ulong ElectionTimerGeneration => _electionTimerGeneration;

    /// <summary>
    /// The heartbeat timer's current generation. A test uses this to construct a deliberately
    /// stale <see cref="HeartbeatIntervalElapsed"/> message.
    /// </summary>
    internal ulong HeartbeatTimerGeneration => _heartbeatTimerGeneration;

    /// <summary>
    /// Arms (or re-arms) the election timer with a fresh randomised delay and increments the
    /// generation counter so any previously scheduled <see cref="ElectionTimeoutElapsed"/> message
    /// is recognised as stale and discarded.
    /// </summary>
    void RestartElectionTimer() {
        var generation = ++_electionTimerGeneration;
        var timeout = NextElectionTimeout();
        ScheduleMessage(timeout, new ElectionTimeoutElapsed(generation));
    }

    /// <summary>
    /// Schedules <paramref name="message"/> to be posted to the actor channel after the given <paramref name="delay"/>.
    /// The delay task is cancelled automatically when the node stops (via <see cref="_cts"/>), so no timer resource is leaked on shutdown.
    /// </summary>
    void ScheduleMessage(TimeSpan delay, RaftReplicaMessage message) {
        var cancellationToken = _cts?.Token ?? CancellationToken.None;
        _ = Task.Run(async () => {
            try {
                await Task.Delay(delay, _timeProvider, cancellationToken);
                _channel.Writer.TryWrite(message);
            } catch (OperationCanceledException) { }
        }, cancellationToken);
    }

    /// <summary>
    /// Returns a uniform random timeout between <c>replicaConfig.ElectionTimeoutMin</c> and <c>replicaConfig.ElectionTimeoutMax</c>.
    /// </summary>
    /// <returns>A <see cref="TimeSpan"/>.</returns>
    TimeSpan NextElectionTimeout() =>
        TimeSpan.FromTicks(_random.NextInt64(
            replicaConfig.ElectionTimeoutMin.Ticks,
            replicaConfig.ElectionTimeoutMax.Ticks
        ));

    async Task HandleElectionTimeoutElapsedAsync(ElectionTimeoutElapsed message) {
        if (message.Generation != _electionTimerGeneration)
            return; // superseded — a newer arm happened after this firing was scheduled

        // Defense-in-depth: with generations this branch should be unreachable (becoming leader
        // re-arms/invalidates the election timer), but a sister project's own term-explosion
        // incident is the canonical reason to keep the explicit role check anyway — it costs
        // nothing and turns a future bookkeeping bug into a no-op instead of a spurious re-election.
        if (_role == ReplicaRole.Leader)
            return;

        await BecomeCandidateAsync();
    }

    /// <summary>
    /// Arms (or re-arms) the heartbeat timer — same generation mechanism as
    /// <see cref="RestartElectionTimer"/>, with the fixed interval instead of a randomised one.
    /// </summary>
    void RestartHeartbeatTimer() {
        var generation = ++_heartbeatTimerGeneration;
        ScheduleMessage(replicaConfig.HeartbeatInterval, new HeartbeatIntervalElapsed(generation));
    }

    // One-shot, re-armed after every tick — sending never overlaps with itself.
    async Task HandleHeartbeatIntervalElapsedAsync(HeartbeatIntervalElapsed message) {
        if (message.Generation != _heartbeatTimerGeneration)
            return; // superseded
        if (_role != ReplicaRole.Leader)
            return; // defense-in-depth, same rationale as the election-timer role guard

        await ReplicateToAllPeersAsync();
        RestartHeartbeatTimer();
    }
}
