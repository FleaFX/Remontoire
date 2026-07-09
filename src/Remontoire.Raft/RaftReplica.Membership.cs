using Google.Protobuf;
using Remontoire.Raft.V1;
using Remontoire.Storage;

namespace Remontoire.Raft;

public sealed partial class RaftReplica {
    /// <summary>
    /// Proposes a full membership change — <paramref name="fullMembership"/> is the complete new
    /// member list (every voting node, including whichever one this happens to be), not a delta.
    /// Effective on this replica the moment it's appended (before commit, per the paper); the
    /// returned task completes only once it's quorum-committed. Throws
    /// <see cref="NotLeaderException"/> when this replica is not the ready leader, or
    /// <see cref="InvalidOperationException"/> when another change is already outstanding — single-
    /// server changes only guarantee safety one at a time, never for two in flight concurrently.
    /// </summary>
    public ValueTask ProposeConfigChangeAsync(IReadOnlyList<RaftGroupMember> fullMembership, CancellationToken cancellationToken = default) {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new ProposeConfigChangeReceived(fullMembership, completion));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask(completion.Task.WaitAsync(cancellationToken));
    }

    async Task HandleProposeConfigChangeReceivedAsync(ProposeConfigChangeReceived message) {
        if (_role != ReplicaRole.Leader || !_isLeaderReady) {
            message.Reply.TrySetException(new NotLeaderException(GroupId, _leaderHint));
            return;
        }

        if (_pendingConfigChangeIndex is { } outstandingIndex) {
            message.Reply.TrySetException(new InvalidOperationException(
                $"A configuration change is already in progress in '{GroupId}' (index {outstandingIndex}) — only one may be outstanding at a time."));
            return;
        }

        var record = new WalRecord(WalRecordType.ShardConfigChange, _currentTerm, raftLog.LastIndex + 1, LogicalOffset: 0,
            TimestampMicros: (ulong)_timeProvider.GetUtcNow().ToUnixTimeMilliseconds() * 1000,
            PartitionKey: ReadOnlyMemory<byte>.Empty, Headers: [], Payload: SerializeMembers(message.FullMembership));

        // Resolves in AdvanceCommitIndexAsync, on quorum commit — never here, matching
        // HandleProposeReceivedAsync's own reply-on-commit rule.
        _pendingConfigChangeReply = message.Reply;

        await raftLog.AppendAsync([record]);
        ApplyConfigChangeIfPresent(record);

        // Single-node-group safety net, mirrors HandleProposeReceivedAsync: with no peers, this
        // is the only place anything re-checks commit progress for this entry.
        await TryAdvanceLeaderCommitAsync();

        await ReplicateToAllPeersAsync();
    }

    // Updates the active configuration the moment a ShardConfigChange is appended — called from
    // both the propose path above and the AppendEntries receive path (RaftReplica.Replication.cs),
    // for the same reason a WalRecord looks identical regardless of who appended it: the paper
    // requires every node to act on the newest configuration in its own log, committed or not.
    void ApplyConfigChangeIfPresent(WalRecord record) {
        if (record.RecordType != WalRecordType.ShardConfigChange)
            return;

        var fullMembership = DeserializeMembers(record.Payload.Span);

        _configurationBeforePending = (_activeConfiguration, _activeConfigurationIndex, _selfIsMember);
        _activeConfiguration = DerivePeers(fullMembership);
        _activeConfigurationIndex = record.RaftIndex;
        _selfIsMember = fullMembership.Any(member => member.NodeId == replicaConfig.NodeId);
        _pendingConfigChangeIndex = record.RaftIndex;

        // A leader whose own peer set just changed must track next/matchIndex for exactly the
        // new membership: an added peer starts exactly like a fresh BecomeLeaderAsync would
        // (nextIndex at the tail); a departed one is simply dropped, nothing left to replicate to.
        if (_role == ReplicaRole.Leader) {
            var nextIndex = new Dictionary<string, ulong>();
            var matchIndex = new Dictionary<string, ulong>();
            var installSnapshotInProgress = new HashSet<string>();

            foreach (var peer in _activeConfiguration) {
                nextIndex[peer.NodeId] = _nextIndex!.GetValueOrDefault(peer.NodeId, raftLog.LastIndex + 1);
                matchIndex[peer.NodeId] = _matchIndex!.GetValueOrDefault(peer.NodeId, 0UL);
                if (_installSnapshotInProgressPeers!.Contains(peer.NodeId))
                    installSnapshotInProgress.Add(peer.NodeId);
            }

            _nextIndex = nextIndex;
            _matchIndex = matchIndex;
            _installSnapshotInProgressPeers = installSnapshotInProgress;
        }
    }

    // Called after every successful TruncateFromAsync (RaftReplica.Replication.cs) — a cheap
    // no-op unless the discarded suffix actually reached back into the active configuration's
    // own entry. "One at a time" (ProposeConfigChangeAsync) guarantees _configurationBeforePending
    // is set whenever that's the case: the active configuration can only be uncommitted (and so
    // truncatable) while its own change is still the one outstanding change being tracked.
    void RevertConfigurationIfTruncated(ulong fromIndex) {
        if (_activeConfigurationIndex < fromIndex)
            return;

        if (_configurationBeforePending is { } previous) {
            _activeConfiguration = previous.Configuration;
            _activeConfigurationIndex = previous.Index;
            _selfIsMember = previous.SelfIsMember;
            _configurationBeforePending = null;
        }

        _pendingConfigChangeIndex = null;
    }

    // Called once at StartAsync. Mirrors RecoverNextLogicalOffsetAsync's shape exactly: floor on
    // the durable snapshot state, then scan forward for anything more recent than the snapshot.
    async Task<(IReadOnlyList<RaftGroupMember> Configuration, ulong Index)> RecoverActiveConfigurationAsync() {
        IReadOnlyList<RaftGroupMember> configuration = _snapshotConfiguration is { } bytes ? DeserializeMembers(bytes) : replicaConfig.Peers;
        var index = 0UL;

        await foreach (var record in raftLog.ReadFromAsync(raftLog.SnapshotIndex + 1)) {
            if (record.RecordType != WalRecordType.ShardConfigChange)
                continue;

            configuration = DerivePeers(DeserializeMembers(record.Payload.Span));
            index = record.RaftIndex;
        }

        return (configuration, index);
    }

    // _activeConfiguration itself (already peers only) — not the raw ShardConfigChange payload
    // (which is self-inclusive, see SerializeMembers) — is what gets persisted here: recovery
    // only ever needs "who are my peers", never needs to re-derive that from a full membership.
    byte[] SerializeSnapshotConfiguration() => SerializeMembers(_activeConfiguration);

    RaftGroupMember[] DerivePeers(IReadOnlyList<RaftGroupMember> fullMembership) =>
        fullMembership.Where(member => member.NodeId != replicaConfig.NodeId).ToArray();

    static byte[] SerializeMembers(IReadOnlyList<RaftGroupMember> members) {
        var proto = new ShardConfiguration();
        foreach (var member in members)
            proto.Members.Add(new ShardConfigMember { NodeId = member.NodeId, Address = member.Address.ToString() });

        return proto.ToByteArray();
    }

    static RaftGroupMember[] DeserializeMembers(ReadOnlySpan<byte> payload) {
        var proto = ShardConfiguration.Parser.ParseFrom(payload);
        return proto.Members.Select(member => new RaftGroupMember(member.NodeId, new Uri(member.Address))).ToArray();
    }
}
