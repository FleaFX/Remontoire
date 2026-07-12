using Microsoft.Extensions.Logging;

namespace Remontoire.Raft;

/// <summary>
/// This node's membership in one physical shard group: a single-mailbox actor that owns all
/// Raft state for that group. Callers may invoke the public API concurrently without locking —
/// every operation is a message posted to the actor's one sequential processing loop.
/// </summary>
public sealed partial class RaftReplica(
    IRaftStateStore stateStore,
    IRaftLog raftLog,
    IRaftTransport transport,
    RaftReplicaConfig replicaConfig,
    TimeProvider? timeProvider = null,
    ILogger<RaftReplica>? logger = null,
    Func<ulong, CancellationToken, Task<IReadOnlyList<string>>>? prepareSnapshot = null,
    Func<IReadOnlyList<string>, ulong, CancellationToken, Task>? installSnapshot = null
) : IAsyncDisposable {
    /// <summary>
    /// The physical shard group this replica belongs to.
    /// </summary>
    public string GroupId { get; } = replicaConfig.GroupId;

    /// <summary>
    /// Whether this replica is the current leader AND has committed its own term-opening NoOp
    /// entry — only then may it accept proposals.
    /// </summary>
    public bool IsLeader => Volatile.Read(ref _isLeaderReady);

    /// <summary>
    /// The node this replica believes is the current leader, or <see langword="null"/> during
    /// an election.
    /// </summary>
    public string? LeaderHint => Volatile.Read(ref _leaderHint);

    /// <summary>
    /// The current Raft term.
    /// </summary>
    public ulong CurrentTerm => Volatile.Read(ref _currentTerm);

    /// <summary>
    /// The highest log index known committed on this replica.
    /// </summary>
    public ulong CommitIndex => Volatile.Read(ref _commitIndex);

    /// <summary>
    /// This group's own election-timeout upper bound — the natural staleness threshold for
    /// diagnostics built on top of <see cref="LastActorLoopActivity"/>/<see cref="HasActiveLeaderContact"/>,
    /// rather than inventing a second, unrelated timing constant.
    /// </summary>
    public TimeSpan ElectionTimeoutMax => replicaConfig.ElectionTimeoutMax;

    /// <summary>
    /// The number of <c>AppendEntries</c> RPCs sent to each peer so far — heartbeats and real
    /// replication both funnel through the single <c>SendAppendEntriesAsync</c> call site, so this
    /// counts both identically.
    /// </summary>
    public IReadOnlyDictionary<string, long> AppendEntriesSentTotal => _appendEntriesSentTotal;

    /// <summary>
    /// The number of times this replica has become leader.
    /// </summary>
    public long LeaderElectionsTotal => Volatile.Read(ref _leaderElectionsTotal);

    /// <summary>
    /// The last time the actor loop processed a message — a liveness signal: this replica's actor
    /// loop is presumed stuck once this falls far enough behind <see cref="ElectionTimeoutMax"/>.
    /// </summary>
    public DateTimeOffset LastActorLoopActivity => new(Volatile.Read(ref _lastActorLoopActivityUtcTicks), TimeSpan.Zero);

    /// <summary>
    /// The active WAL file's own fsync-in-progress diagnostic (<see cref="WalRaftLog.FlushInProgressSince"/>),
    /// or <see langword="null"/> for any <see cref="IRaftLog"/> that isn't a <see cref="WalRaftLog"/>
    /// (e.g. test fakes) — those have no comparable notion of a flush to report on.
    /// </summary>
    public DateTimeOffset? WalFlushInProgressSince => (raftLog as WalRaftLog)?.FlushInProgressSince;

    /// <summary>
    /// Whether this replica has recently accepted an <c>AppendEntries</c> from a current leader —
    /// <see langword="false"/> both when it never has, and when the last acceptance is older than
    /// <see cref="ElectionTimeoutMax"/>. Meaningless (always <see langword="false"/>) while this
    /// replica is itself the leader — see <see cref="IsLeader"/> for that case instead.
    /// </summary>
    public bool HasActiveLeaderContact {
        get {
            var ticks = Volatile.Read(ref _lastLeaderContactUtcTicks);
            return ticks != 0 && _timeProvider.GetUtcNow() - new DateTimeOffset(ticks, TimeSpan.Zero) <= replicaConfig.ElectionTimeoutMax;
        }
    }

    /// <summary>
    /// The <c>LeaderCommit</c> carried by the last accepted <c>AppendEntries</c> — combined with
    /// <see cref="CommitIndex"/>, the replication-lag gauge's two source values. Meaningless on a
    /// leader itself, which already knows its own commit index directly.
    /// </summary>
    public ulong LeaderKnownCommitIndex => Volatile.Read(ref _leaderKnownCommitIndex);

    /// <summary>
    /// Loads durable state, starts the actor loop and arms the election timer.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default) {
        // Stamped here too, not only per-message inside RunActorLoopAsync: without this, a
        // just-started replica reads as "stuck" (LastActorLoopActivity at its zero/epoch default)
        // for the brief window between StartAsync returning and its first election-timer message
        // actually arriving — a healthy, merely-idle replica must never appear stuck.
        Volatile.Write(ref _lastActorLoopActivityUtcTicks, _timeProvider.GetUtcNow().Ticks);

        (_currentTerm, _votedFor, _snapshotNextLogicalOffset, _snapshotConfiguration) = await stateStore.LoadAsync(cancellationToken);
        (_activeConfiguration, _activeConfigurationIndex) = await RecoverActiveConfigurationAsync();

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _actorLoopTask = Task.Run(() => RunActorLoopAsync(_cts.Token), cancellationToken);

        // Assign role before arming the timer so the first timer message is dispatched correctly.
        _role = ReplicaRole.Follower;
        RestartElectionTimer();
    }

    /// <summary>
    /// Cancels the actor loop, awaits shutdown, and fails any pending proposals.
    /// </summary>
    internal async Task StopAsync(CancellationToken cancellationToken = default) {
        if (_cts is null) return;
        await _cts.CancelAsync();

        if (_actorLoopTask is not null)
            await _actorLoopTask
                .WaitAsync(cancellationToken)
                .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        FailPendingProposals();
        _channel.Writer.TryComplete();
    }

    // Unlike ShardLog.RunActorAsync (which validates at the API edge and keeps the loop bare),
    // this has a try/catch per message: a bug in one handler must never kill the whole replica
    // loop — a dead loop is a silent node, exactly the failure mode this actor design exists to
    // rule out.
    async Task RunActorLoopAsync(CancellationToken cancellationToken) {
        await foreach (var message in _channel.Reader.ReadAllAsync(cancellationToken)) {
            Volatile.Write(ref _lastActorLoopActivityUtcTicks, _timeProvider.GetUtcNow().Ticks);
            try {
                await (message switch {
                    AppendEntriesReceived appendEntriesReceived => HandleAppendEntriesReceivedAsync(appendEntriesReceived),
                    AppendEntriesResponseReceived appendEntriesResponseReceived => HandleAppendEntriesResponseReceivedAsync(appendEntriesResponseReceived),
                    DrainSentinel drainSentinel => HandleDrainSentinel(drainSentinel),
                    ElectionTimeoutElapsed electionTimeoutElapsed => HandleElectionTimeoutElapsedAsync(electionTimeoutElapsed),
                    HeartbeatIntervalElapsed heartbeatIntervalElapsed => HandleHeartbeatIntervalElapsedAsync(heartbeatIntervalElapsed),
                    InstallSnapshotReceived installSnapshotReceived => HandleInstallSnapshotReceivedAsync(installSnapshotReceived),
                    InstallSnapshotResponseReceived installSnapshotResponseReceived => HandleInstallSnapshotResponseReceivedAsync(installSnapshotResponseReceived),
                    InstallSnapshotTransferFailed installSnapshotTransferFailed => HandleInstallSnapshotTransferFailedAsync(installSnapshotTransferFailed),
                    ProposeAckCheckpointReceived proposeAckCheckpointReceived => HandleProposeAckCheckpointReceivedAsync(proposeAckCheckpointReceived),
                    ProposeAckReceived proposeAckReceived => HandleProposeAckReceivedAsync(proposeAckReceived),
                    ProposeConfigChangeReceived proposeConfigChangeReceived => HandleProposeConfigChangeReceivedAsync(proposeConfigChangeReceived),
                    ProposeReceived proposeReceived => HandleProposeReceivedAsync(proposeReceived),
                    SnapshotPrepared snapshotPrepared => HandleSnapshotPreparedAsync(snapshotPrepared),
                    SnapshotPreparationFailed snapshotPreparationFailed => HandleSnapshotPreparationFailedAsync(snapshotPreparationFailed),
                    VoteRequestReceived voteRequestReceived => HandleVoteRequestReceivedAsync(voteRequestReceived),
                    VoteResponseReceived voteResponseReceived => HandleVoteResponseReceivedAsync(voteResponseReceived),
                    _ => Task.CompletedTask
                });
            } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
                break;
            } catch (Exception ex) {
                logger?.LogError(ex, "Unhandled error processing {MessageType} in {ShardGroupId}", message.GetType().Name, GroupId);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync() => await StopAsync();
}
