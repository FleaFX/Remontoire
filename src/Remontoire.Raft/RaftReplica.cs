using Microsoft.Extensions.Logging;
using Remontoire.Raft.V1;
using Remontoire.Storage;

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
    ILogger<RaftReplica>? logger = null
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
        (_currentTerm, _votedFor) = await stateStore.LoadAsync(cancellationToken);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _actorLoopTask = Task.Run(() => RunActorLoopAsync(_cts.Token), cancellationToken);

        // Assign role before arming the timer so the first timer message is dispatched correctly.
        _role = ReplicaRole.Follower;
        RestartElectionTimer();
    }

    /// <summary>
    /// Proposes one record for replication — a message post plus an awaited reply, exactly
    /// <c>ShardLog.AppendAsync</c>'s shape. Completes on quorum commit, never on mere local
    /// durability. Throws <see cref="NotLeaderException"/> when this replica is not the ready
    /// leader (see <see cref="IsLeader"/>).
    /// </summary>
    public ValueTask<ProposeResult> ProposeAsync(AppendRequest request, CancellationToken cancellationToken = default) {
        var completion = new TaskCompletionSource<ProposeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new ProposeReceived(request, completion));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<ProposeResult>(completion.Task.WaitAsync(cancellationToken));
    }

    /// <summary>
    /// Yields every record in the exact order it became quorum-committed on this replica. Only
    /// one consumer may enumerate this at a time.
    /// </summary>
    public IAsyncEnumerable<WalRecord> ReadCommittedAsync(CancellationToken cancellationToken = default) =>
        _committed.Reader.ReadAllAsync(cancellationToken);

    /// <summary>
    /// Inbound RPC entry point — the transport posts the request as a mailbox message; the actor
    /// resolves the reply only after the persist-before-respond ordering is satisfied.
    /// </summary>
    public ValueTask<VoteResponse> ReceiveVoteRequestAsync(VoteRequest request, CancellationToken cancellationToken = default) {
        var reply = new TaskCompletionSource<VoteResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new VoteRequestReceived(request, reply));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<VoteResponse>(reply.Task.WaitAsync(cancellationToken));
    }

    /// <inheritdoc cref="ReceiveVoteRequestAsync"/>
    public ValueTask<AppendEntriesResponse> ReceiveAppendEntriesAsync(AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        var reply = new TaskCompletionSource<AppendEntriesResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new AppendEntriesReceived(request, reply));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<AppendEntriesResponse>(reply.Task.WaitAsync(cancellationToken));
    }

    /// <inheritdoc cref="ReceiveVoteRequestAsync"/>
    public ValueTask<InstallSnapshotResponse> ReceiveInstallSnapshotAsync(InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        var reply = new TaskCompletionSource<InstallSnapshotResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new InstallSnapshotReceived(request, reply));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<InstallSnapshotResponse>(reply.Task.WaitAsync(cancellationToken));
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
                    ProposeReceived proposeReceived => HandleProposeReceivedAsync(proposeReceived),
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

    static Task HandleDrainSentinel(DrainSentinel sentinel) {
        sentinel.Completion.TrySetResult();
        return Task.CompletedTask;
    }

    // Snapshots + InstallSnapshot are not implemented yet. Left as an explicit gap rather than a
    // silent no-op: a caller that invokes this before it's built will see the failure surface
    // (logged, reply never resolves) instead of a misleadingly "successful" response.
    Task HandleInstallSnapshotReceivedAsync(InstallSnapshotReceived message) =>
        throw new NotImplementedException("InstallSnapshot is not implemented yet.");

    Task HandleInstallSnapshotResponseReceivedAsync(InstallSnapshotResponseReceived message) =>
        throw new NotImplementedException("InstallSnapshot is not implemented yet.");

    /// <inheritdoc />
    public async ValueTask DisposeAsync() => await StopAsync();
}
