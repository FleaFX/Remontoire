using Remontoire.Raft.V1;
using Remontoire.Storage;
using Remontoire.Storage.Serialization;

namespace Remontoire.Raft;

public sealed partial class RaftReplica {
    /// <summary>
    /// Proposes one record for replication — a message post plus an awaited reply. Completes on
    /// quorum commit, never on mere local durability. Throws <see cref="NotLeaderException"/>
    /// when this replica is not the ready leader (see <see cref="IsLeader"/>).
    /// </summary>
    public ValueTask<ProposeResult> ProposeAsync(AppendRequest request, CancellationToken cancellationToken = default) {
        ValidateRequest(request);

        var completion = new TaskCompletionSource<ProposeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new ProposeReceived(request, completion));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<ProposeResult>(completion.Task.WaitAsync(cancellationToken));
    }

    // PartitionKey/header keys are encoded with a 16-bit length prefix on disk (WalRecordSerializer,
    // via VariableLengthTail) — validated here, at the public API boundary, rather than deep in the
    // serializer or on the actor loop: an exception escaping HandleProposeReceivedAsync would crash
    // the actor loop itself (nobody observes that Task until DisposeAsync), hanging every future
    // proposal instead of failing this one cleanly. Payload/header values use a 32-bit prefix
    // (~4 GB) — not validated, an unreachable scenario in practice.
    static void ValidateRequest(AppendRequest request) {
        if (request.PartitionKey.Length > ushort.MaxValue)
            throw new ArgumentException($"PartitionKey is {request.PartitionKey.Length} bytes, exceeds the 16-bit length-prefix limit of {ushort.MaxValue}.", nameof(request));

        if (request.Headers.Count > ushort.MaxValue)
            throw new ArgumentException($"Request has {request.Headers.Count} headers, exceeds the 16-bit count-prefix limit of {ushort.MaxValue}.", nameof(request));

        foreach (var header in request.Headers) {
            if (header.Key.Length > ushort.MaxValue)
                throw new ArgumentException($"A header key is {header.Key.Length} bytes, exceeds the 16-bit length-prefix limit of {ushort.MaxValue}.", nameof(request));
        }
    }

    /// <summary>
    /// Yields every record in the exact order it became quorum-committed on this replica. Only
    /// one consumer may enumerate this at a time.
    /// </summary>
    public IAsyncEnumerable<WalRecord> ReadCommittedAsync(CancellationToken cancellationToken = default) =>
        _committed.Reader.ReadAllAsync(cancellationToken);

    /// <inheritdoc cref="ReceiveVoteRequestAsync"/>
    public ValueTask<AppendEntriesResponse> ReceiveAppendEntriesAsync(AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        var reply = new TaskCompletionSource<AppendEntriesResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new AppendEntriesReceived(request, reply));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<AppendEntriesResponse>(reply.Task.WaitAsync(cancellationToken));
    }

    async Task HandleProposeReceivedAsync(ProposeReceived message) {
        if (_role != ReplicaRole.Leader || !_isLeaderReady) {
            message.Reply.TrySetException(new NotLeaderException(GroupId, _leaderHint));
            return;
        }

        var request = message.Request;
        var record = new WalRecord(WalRecordType.Append, _currentTerm, raftLog.LastIndex + 1, _nextLogicalOffset++,
            (ulong)_timeProvider.GetUtcNow().ToUnixTimeMilliseconds() * 1000,
            request.PartitionKey, request.Headers, request.Payload);

        // The reply resolves in AdvanceCommitIndexAsync, on quorum commit — never here: no
        // LogicalOffset escapes the replica before its entry is committed.
        _pendingProposals!.Add(record.RaftIndex, new PendingProposal(new ProposeResult(record.RaftIndex, record.LogicalOffset), message.Reply));

        // Durable local append before replication: the leader's own entry counts toward the
        // quorum, and it may only count once fsynced.
        await raftLog.AppendAsync([record]);

        // For a single-node group (no peers) this is the ONLY place anything ever re-checks
        // commit progress for this entry — there is no peer AppendEntriesResponseReceived to
        // trigger TryAdvanceLeaderCommitAsync otherwise, and ProposeAsync would hang forever
        // without this call.
        await TryAdvanceLeaderCommitAsync();

        await ReplicateToAllPeersAsync();
    }

    async Task HandleAppendEntriesReceivedAsync(AppendEntriesReceived message) {
        var request = message.Request;

        // (1) A stale leader is refused with our term — it will step down on seeing it.
        if (request.Term < _currentTerm) {
            message.Reply.TrySetResult(new AppendEntriesResponse { Term = _currentTerm, Success = false, ConflictIndex = 0, ConflictTerm = 0 });
            return;
        }

        // (2) Equal-or-higher term: this is the current leader. A candidate at the same term
        // yields (someone else won this election); a second leader at the same term is
        // impossible (Election Safety). Either path re-arms the election timer — this IS the
        // heartbeat.
        if (request.Term > _currentTerm || _role != ReplicaRole.Follower) {
            await BecomeFollowerAsync(request.Term, request.LeaderId);
        } else {
            Volatile.Write(ref _leaderHint, request.LeaderId);
            RestartElectionTimer();
        }

        // (3) Log-matching consistency check: our entry at PrevLogIndex must carry PrevLogTerm.
        // Too short a log: point the leader straight at our end.
        if (request.PrevLogIndex > raftLog.LastIndex) {
            message.Reply.TrySetResult(new AppendEntriesResponse { Term = _currentTerm, Success = false, ConflictIndex = raftLog.LastIndex + 1, ConflictTerm = 0 });
            return;
        }

        if (request.PrevLogIndex > raftLog.SnapshotIndex) {
            var localPrevTerm = await raftLog.GetTermAtAsync(request.PrevLogIndex);
            if (localPrevTerm != request.PrevLogTerm) {
                // Conflict acceleration: report our conflicting term and our first index of that
                // term, so the leader skips the whole term in one step (paper §5.3).
                var conflictIndex = request.PrevLogIndex;
                while (conflictIndex - 1 > raftLog.SnapshotIndex && await raftLog.GetTermAtAsync(conflictIndex - 1) == localPrevTerm)
                    conflictIndex--;

                message.Reply.TrySetResult(new AppendEntriesResponse { Term = _currentTerm, Success = false, ConflictIndex = conflictIndex, ConflictTerm = localPrevTerm });
                return;
            }
        }

        // Decode the wire-encoded entries once, up front — each owns a pooled buffer that must
        // be released once we're done with it, success or failure alike.
        var decoded = new List<WalReadResult>(request.Entries.Count);
        try {
            foreach (var entryBytes in request.Entries) {
                var result = WalRecordSerializer.TryRead(entryBytes.Span);
                if (result.Status != WalRecordReadStatus.Success)
                    throw new InvalidDataException($"Received a corrupt or incomplete WAL record over AppendEntries ({result.Status}).");

                decoded.Add(result);
            }

            // (4) Skip entries we already hold; truncate only on a genuine term conflict. A
            // delayed or duplicated request must never rewrite existing entries. Once the first
            // new or conflicting entry is found, everything after it is new by definition —
            // hence the newEntries.Count guard: raftLog.LastIndex is stale for the remainder of
            // this loop after a truncation.
            var newEntries = new List<WalRecord>();
            foreach (var decodedEntry in decoded) {
                var entry = decodedEntry.Record;
                if (newEntries.Count == 0 && entry.RaftIndex <= raftLog.LastIndex) {
                    if (await raftLog.GetTermAtAsync(entry.RaftIndex) == entry.RaftTerm)
                        continue; // already have it — a re-delivery, not a conflict

                    // Conflicting suffix: ours loses. Never below the commit index — committed
                    // entries cannot conflict with the current leader.
                    await raftLog.TruncateFromAsync(entry.RaftIndex);
                }
                newEntries.Add(entry);
            }

            // (5) Durable append BEFORE the reply below.
            if (newEntries.Count > 0)
                await raftLog.AppendAsync(newEntries);

            // (6) Commit follows the leader, bounded by what this request actually confirmed.
            var lastNewIndex = request.PrevLogIndex + (ulong)request.Entries.Count;
            await AdvanceCommitIndexAsync(Math.Min(request.LeaderCommit, lastNewIndex));

            message.Reply.TrySetResult(new AppendEntriesResponse { Term = _currentTerm, Success = true, ConflictIndex = 0, ConflictTerm = 0 });
        } finally {
            foreach (var result in decoded)
                result.Dispose();
        }
    }

    // The leader-side response handler, and the commit path itself. The commit rule, exactly:
    // advance commitIndex to N only when a majority has matchIndex ≥ N AND term(N) ==
    // currentTerm (Raft Figure 8 — entries of older terms only commit implicitly, never on their
    // own majority replication).
    async Task HandleAppendEntriesResponseReceivedAsync(AppendEntriesResponseReceived message) {
        if (message.SentTerm != _currentTerm || _role != ReplicaRole.Leader)
            return; // stale by construction

        if (message.Response.Term > _currentTerm) {
            await BecomeFollowerAsync(message.Response.Term, leaderHint: null);
            return;
        }

        if (message.Response.Success) {
            // Monotonic max: a reordered older response must never move either cursor backwards.
            if (message.SentUpToIndex > _matchIndex![message.PeerId]) {
                _matchIndex[message.PeerId] = message.SentUpToIndex;
                _nextIndex![message.PeerId] = message.SentUpToIndex + 1;
                await TryAdvanceLeaderCommitAsync();
            }
            return;
        }

        // Log inconsistency: back nextIndex up using the conflict hint. When we hold entries of
        // the conflicting term ourselves, resume right after our last one; otherwise jump to the
        // follower's first index of that term (or its log end) — paper §5.3, leader side.
        var nextIndex = message.Response.ConflictIndex;
        if (message.Response.ConflictTerm != 0) {
            for (var index = Math.Min(message.Response.ConflictIndex, raftLog.LastIndex); index > raftLog.SnapshotIndex; index--) {
                var term = await raftLog.GetTermAtAsync(index);
                if (term == message.Response.ConflictTerm) {
                    nextIndex = index + 1;
                    break;
                }
                if (term < message.Response.ConflictTerm)
                    break; // terms are monotonic in the log — the conflicting term is not here
            }
        }

        _nextIndex![message.PeerId] = Math.Max(1UL, nextIndex);
        await SendAppendEntriesAsync(message.PeerId); // retry now — don't sit out a heartbeat tick
    }

    /// <summary>
    /// The single place commitIndex ever advances, in both roles, and therefore the single
    /// feeder of the <see cref="RaftReplica.ReadCommittedAsync"/> stream.
    /// </summary>
    async Task AdvanceCommitIndexAsync(ulong newCommitIndex) {
        if (newCommitIndex <= _commitIndex)
            return;

        var firstNewlyCommitted = _commitIndex + 1;
        Volatile.Write(ref _commitIndex, newCommitIndex);

        // Publish the newly committed range in log order — the storage side applies from this
        // feed and nothing else.
        await foreach (var record in raftLog.ReadFromAsync(firstNewlyCommitted)) {
            if (record.RaftIndex > newCommitIndex)
                break;
            _committed.Writer.TryWrite(record);
        }

        if (_role != ReplicaRole.Leader)
            return;

        // The leader becomes ready only once its own term-opening NoOp is in the committed prefix.
        if (!_isLeaderReady && newCommitIndex >= _leaderNoOpIndex)
            Volatile.Write(ref _isLeaderReady, true);

        // Resolve every proposal whose entry is now committed — the first moment its
        // LogicalOffset may become visible outside the replica.
        while (_pendingProposals!.Count > 0) {
            var (raftIndex, pending) = _pendingProposals.First();
            if (raftIndex > newCommitIndex)
                break;

            _pendingProposals.Remove(raftIndex);
            pending.Reply.TrySetResult(pending.Result);
        }
    }
}
