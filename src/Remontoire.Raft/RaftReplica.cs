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
    /// Loads durable state, starts the actor loop and arms the election timer.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default) {
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
