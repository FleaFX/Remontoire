using System.Buffers;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Remontoire.Raft.V1;
using Remontoire.Storage;
using Remontoire.Storage.Serialization;

namespace Remontoire.Raft;

public sealed partial class RaftReplica {
    /// <summary>
    /// Transitions to leader. <see cref="IsLeader"/> deliberately stays <see langword="false"/>
    /// here: it flips in <see cref="AdvanceCommitIndexAsync"/>, once the term-opening NoOp below
    /// is quorum-committed — never earlier.
    /// </summary>
    async Task BecomeLeaderAsync() {
        Interlocked.Increment(ref _leaderElectionsTotal);
        _role = ReplicaRole.Leader;
        Volatile.Write(ref _leaderHint, replicaConfig.NodeId);
        _electionTimerGeneration++; // a leader has no election timeout — invalidate without re-arming

        _nextIndex = _activeConfiguration.ToDictionary(peer => peer.NodeId, _ => raftLog.LastIndex + 1);
        _matchIndex = _activeConfiguration.ToDictionary(peer => peer.NodeId, _ => 0UL);
        _installSnapshotInProgressPeers = [];
        _pendingProposals = new SortedDictionary<ulong, PendingProposal>();
        _nextLogicalOffset = await RecoverNextLogicalOffsetAsync();

        // Leader-completeness anchor: an own-term entry that, once committed, implicitly
        // commits every earlier-term entry beneath it. NoOps consume no logical offset — the
        // consumer-visible sequence stays gapless.
        _leaderNoOpIndex = raftLog.LastIndex + 1;
        var noOp = new WalRecord(WalRecordType.NoOp, _currentTerm, _leaderNoOpIndex, LogicalOffset: 0,
            TimestampMicros: (ulong)_timeProvider.GetUtcNow().ToUnixTimeMilliseconds() * 1000,
            PartitionKey: ReadOnlyMemory<byte>.Empty, Headers: [], Payload: ReadOnlyMemory<byte>.Empty);
        await raftLog.AppendAsync([noOp]);

        // A single-node group is its own quorum: the NoOp commits on this append alone, and
        // IsLeader flips before the first proposal can even arrive.
        await TryAdvanceLeaderCommitAsync();

        await ReplicateToAllPeersAsync();
        RestartHeartbeatTimer();
    }

    async Task ReplicateToAllPeersAsync() {
        foreach (var peer in _activeConfiguration)
            await SendAppendEntriesAsync(peer.NodeId);
    }

    async Task SendAppendEntriesAsync(string peerId) {
        _appendEntriesSentTotal.AddOrUpdate(peerId, 1, (_, count) => count + 1);

        var nextIndex = _nextIndex![peerId];
        if (nextIndex <= raftLog.SnapshotIndex) {
            // The peer needs entries that were compacted away — AppendEntries cannot help.
            await SendInstallSnapshotAsync(peerId);
            return;
        }

        var prevLogIndex = nextIndex - 1;
        var prevLogTerm = prevLogIndex == 0 ? 0UL : await raftLog.GetTermAtAsync(prevLogIndex);

        // Everything from nextIndex up to the tail. No per-request cap in phase 3: the tail is
        // snapshot-bounded, and a peer lagging beyond that takes the snapshot path above.
        var request = new AppendEntriesRequest {
            GroupId = replicaConfig.GroupId,
            Term = _currentTerm,
            LeaderId = replicaConfig.NodeId,
            PrevLogIndex = prevLogIndex,
            PrevLogTerm = prevLogTerm,
            LeaderCommit = _commitIndex,
        };

        var sentUpToIndex = prevLogIndex;
        await foreach (var record in raftLog.ReadFromAsync(nextIndex)) {
            request.Entries.Add(EncodeEntry(record));
            sentUpToIndex = record.RaftIndex;
        }

        var cancellationToken = _cts?.Token ?? CancellationToken.None;
        _ = Task.Run(async () => {
            try {
                var response = await transport.AppendEntriesAsync(peerId, request, cancellationToken);
                _channel.Writer.TryWrite(new AppendEntriesResponseReceived(peerId, response, request.Term, sentUpToIndex));
            } catch (Exception ex) {
                // A failed RPC needs no reaction here: the next heartbeat tick retries from the
                // same nextIndex, and the RPC deadline guarantees this task ends.
                logger?.LogDebug(ex, "AppendEntries to {PeerId} failed in {ShardGroupId}", peerId, GroupId);
            }
        }, cancellationToken);
    }

    static ByteString EncodeEntry(WalRecord record) {
        var length = WalRecordSerializer.GetEncodedLength(record);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try {
            var written = WalRecordSerializer.Write(record, buffer);
            return ByteString.CopyFrom(buffer.AsSpan(0, written));
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    async ValueTask<ulong> RecoverNextLogicalOffsetAsync() {
        var next = _snapshotNextLogicalOffset;
        await foreach (var record in raftLog.ReadFromAsync(raftLog.SnapshotIndex + 1)) {
            if (record.RecordType == WalRecordType.Append)
                next = record.LogicalOffset + 1;
        }

        return next;
    }

    /// <summary>
    /// The leader commit rule, exactly Figure 8: advance to the highest N with a majority at
    /// matchIndex ≥ N AND term(N) == currentTerm — an older-term entry never commits on its own
    /// majority, only implicitly under an own-term entry above it.
    /// </summary>
    async Task TryAdvanceLeaderCommitAsync() {
        for (var candidate = raftLog.LastIndex; candidate > _commitIndex; candidate--) {
            // This replica always matches its own (fsynced) log; count it alongside the peers.
            var replicated = 1 + _matchIndex!.Values.Count(matchIndex => matchIndex >= candidate);
            if (replicated < QuorumSize)
                continue; // not enough replicas this far — try lower

            if (await raftLog.GetTermAtAsync(candidate) != _currentTerm)
                return; // a majority, but an earlier-term entry — wait for an own-term entry on top

            await AdvanceCommitIndexAsync(candidate);
            return;
        }
    }
}
