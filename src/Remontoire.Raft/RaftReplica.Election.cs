using Microsoft.Extensions.Logging;
using Remontoire.Raft.V1;

namespace Remontoire.Raft;

public sealed partial class RaftReplica {
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

    // The single implementation of Raft's election restriction (paper §5.4.1). Everything that
    // ever answers "is that log at least as up-to-date as mine?" calls this — never inline
    // re-derivations: two divergent copies of this comparison can elect a leader that lacks
    // committed entries, which silently loses acknowledged messages.
    static bool IsAtLeastAsUpToDate(ulong candidateLastTerm, ulong candidateLastIndex, ulong ownLastTerm, ulong ownLastIndex) =>
        candidateLastTerm > ownLastTerm
        || (candidateLastTerm == ownLastTerm && candidateLastIndex >= ownLastIndex);

    async Task HandleVoteRequestReceivedAsync(VoteRequestReceived message) {
        var request = message.Request;

        // (1) A stale term is refused outright — with our term, so the candidate can catch up.
        if (request.Term < _currentTerm) {
            message.Reply.TrySetResult(new VoteResponse { Term = _currentTerm, VoteGranted = false });
            return;
        }

        // (2) A higher term makes us a follower on that term. The vote itself is NOT granted by
        // this — that decision follows below, against the (now cleared) _votedFor.
        if (request.Term > _currentTerm)
            await BecomeFollowerAsync(request.Term, leaderHint: null);

        // (3) One vote per term, and only for a candidate whose log is at least as up-to-date —
        // the single implementation above, never re-derived inline.
        var grant = (_votedFor is null || _votedFor == request.CandidateId)
            && IsAtLeastAsUpToDate(request.LastLogTerm, request.LastLogIndex, raftLog.LastTerm, raftLog.LastIndex);

        // (4) Persist before replying. A re-delivered request from the candidate we already
        // voted for grants again without a second save — the vote is already durable.
        if (grant && _votedFor is null) {
            _votedFor = request.CandidateId;
            await stateStore.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor, _snapshotNextLogicalOffset));
        }

        if (grant)
            RestartElectionTimer(); // a legitimate election is in progress — hold off on our own

        message.Reply.TrySetResult(new VoteResponse { Term = _currentTerm, VoteGranted = grant });
    }

    /// <summary>
    /// The candidate transition. The persist happens before anything else — the self-vote falls
    /// under the same durable-before-respond rule as a vote to another candidate: a candidate
    /// that crashes after sending vote requests but before its own term/vote is durable could
    /// vote again in the same term after restart. The election timer is re-armed immediately in
    /// case the election stays undecided (split vote), and a single-node group is an explicit
    /// path — no RPCs to wait on.
    /// </summary>
    async Task BecomeCandidateAsync() {
        Volatile.Write(ref _currentTerm, _currentTerm + 1);
        _votedFor = replicaConfig.NodeId;
        await stateStore.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor, _snapshotNextLogicalOffset));

        _role = ReplicaRole.Candidate;
        Volatile.Write(ref _leaderHint, null);
        _votesReceived = 1; // the self-vote

        // Re-arm now, for the round after this one: an undecided election (split vote) resolves
        // itself through this timer firing again, with a fresh randomised timeout.
        RestartElectionTimer();

        // A single-node group wins on the self-vote alone.
        if (_votesReceived >= QuorumSize) {
            await BecomeLeaderAsync();
            return;
        }

        var request = new VoteRequest {
            GroupId = replicaConfig.GroupId,
            Term = _currentTerm,
            CandidateId = replicaConfig.NodeId,
            LastLogIndex = raftLog.LastIndex,
            LastLogTerm = raftLog.LastTerm,
        };

        foreach (var peer in replicaConfig.Peers)
            SendVoteRequest(peer.NodeId, request);
    }

    // Fires one vote request as a background task: the loop never awaits the network; the
    // response comes back as a mailbox message stamped with the term it left under.
    void SendVoteRequest(string peerId, VoteRequest request) {
        var cancellationToken = _cts?.Token ?? CancellationToken.None;
        _ = Task.Run(async () => {
            try {
                var response = await transport.RequestVoteAsync(peerId, request, cancellationToken);
                _channel.Writer.TryWrite(new VoteResponseReceived(peerId, response, request.Term));
            } catch (Exception ex) {
                // A dropped or timed-out RPC needs no reaction: the election either completes on
                // other votes, or the already re-armed election timer starts the next round.
                logger?.LogDebug(ex, "Vote request to {PeerId} failed in {ShardGroupId}", peerId, GroupId);
            }
        }, cancellationToken);
    }

    // The first guard is the out-of-loop-RPC staleness rule in action: anything whose SentTerm
    // no longer matches, or whose role is no longer Candidate, is stale by construction — a
    // response from a previous election can never touch state again, with no special-casing.
    async Task HandleVoteResponseReceivedAsync(VoteResponseReceived message) {
        if (message.SentTerm != _currentTerm || _role != ReplicaRole.Candidate)
            return; // stale by construction

        if (message.Response.Term > _currentTerm) {
            await BecomeFollowerAsync(message.Response.Term, leaderHint: null);
            return;
        }

        if (!message.Response.VoteGranted)
            return;

        _votesReceived++;
        if (_votesReceived >= QuorumSize)
            await BecomeLeaderAsync();
    }
}
