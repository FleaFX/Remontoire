using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// Drives one reshard operation's steps against real Raft groups — a bare, in-process
/// orchestrator standing in for a later phase's real, authorized admin-API surface; this is at
/// least enough to trigger and verify a migration end-to-end today. Each step is its own method,
/// matching the design's own per-step crash-safety analysis: a caller (an operator tool, or a
/// test injecting a crash between two steps) drives them one at a time, never all-or-nothing.
/// </summary>
public sealed class ReshardOrchestrator(RaftReplicaRegistry raftRegistry, MessagingGroupRegistry messagingRegistry, MigrationAdmissionGate admissionGate) {
    /// <summary>
    /// Step 1 — proposes the migration's start to the meta-group. Routing is unaffected until a
    /// matching <see cref="ProposeCutoverAsync"/> commits.
    /// </summary>
    public async Task ProposeMigrationStartedAsync(
        RaftReplica metaReplica, MigrationId migrationId, string streamName, int virtualShardIndex, string fromGroupId, string toGroupId, CancellationToken cancellationToken = default) =>
        await metaReplica.ProposeAsync(
            new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new MigrationStarted(migrationId, streamName, virtualShardIndex, fromGroupId, toGroupId))),
            cancellationToken);

    /// <summary>
    /// Steps 2-3 — copies every record from <paramref name="fromGroupId"/>'s log starting at
    /// <paramref name="fromOffset"/> into <paramref name="toGroupId"/>'s own log, by re-proposing
    /// each one through its own consensus — never a raw file copy that would bypass it.
    /// Returns the offset copying reached, so a caller can invoke this again for the next delta:
    /// the same mechanism serves both the initial bulk copy and every later tail-catch-up round,
    /// since both are "copy whatever's new since last time."
    /// </summary>
    public async Task<ulong> CopyRecordsAsync(string fromGroupId, string toGroupId, ulong fromOffset, CancellationToken cancellationToken = default) {
        if (!messagingRegistry.TryGet(fromGroupId, out var source))
            throw new InvalidOperationException($"Group '{fromGroupId}' is not hosted here.");
        if (!raftRegistry.TryGet(toGroupId, out var destination))
            throw new InvalidOperationException($"Group '{toGroupId}' is not hosted here.");

        var nextOffset = fromOffset;
        await foreach (var handle in source.ShardLog.ReadFromAsync(fromOffset, cancellationToken)) {
            using (handle)
                await destination.ProposeAsync(new AppendRequest(handle.Entry.PartitionKey, handle.Entry.Headers, handle.Entry.Payload), cancellationToken);

            nextOffset = handle.Entry.LogicalOffset + 1;
        }

        return nextOffset;
    }

    /// <summary>
    /// Step 4 — pauses admission on <paramref name="groupId"/> until the returned scope is
    /// disposed. Local only, deliberately never itself Raft-committed — a leader crash during the
    /// pause simply loses the in-memory pause along with leadership, no cutover having committed.
    /// </summary>
    /// <remarks>
    /// The caller must keep this scope alive through <see cref="ProposeCutoverAsync"/> and until
    /// this group's own locally-materialized assignment table has observed the new routing —
    /// only then should the scope be disposed. Resuming any earlier reopens the exact window this
    /// pause exists to close: a write could land here, quorum-committed on this group, after the
    /// meta-group already considers the shard moved and no further copy round is coming to save
    /// it — a real, if narrow, message-loss window, not merely a style preference.
    /// </remarks>
    public IDisposable PauseAdmission(string groupId) {
        admissionGate.Pause(groupId);
        return new AdmissionPauseScope(admissionGate, groupId);
    }

    /// <summary>
    /// Step 5 — proposes the single, atomic routing flip. The only step that actually changes
    /// routing; rejected by the applier if <paramref name="migrationId"/> doesn't match the
    /// migration currently in progress for this shard. Call this only while the old group's
    /// <see cref="PauseAdmission"/> scope is still active — see that method's own remarks for why.
    /// </summary>
    public async Task ProposeCutoverAsync(RaftReplica metaReplica, MigrationId migrationId, string streamName, int virtualShardIndex, string toGroupId, CancellationToken cancellationToken = default) =>
        await metaReplica.ProposeAsync(
            new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new Cutover(migrationId, streamName, virtualShardIndex, toGroupId))),
            cancellationToken);

    /// <summary>
    /// Step 6 — marks the migration's cleanup as done, after its grace period. Carries no table
    /// change of its own; actual disk reclamation of the old group's copy is a separate concern
    /// this method doesn't perform.
    /// </summary>
    public async Task ProposeMigrationCompletedAsync(RaftReplica metaReplica, MigrationId migrationId, string streamName, int virtualShardIndex, CancellationToken cancellationToken = default) =>
        await metaReplica.ProposeAsync(
            new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new MigrationCompleted(migrationId, streamName, virtualShardIndex))),
            cancellationToken);

    /// <summary>
    /// Aborts an in-progress migration — symmetric with <see cref="ProposeMigrationStartedAsync"/>,
    /// leaving routing exactly where it was.
    /// </summary>
    public async Task ProposeMigrationAbortedAsync(RaftReplica metaReplica, MigrationId migrationId, string streamName, int virtualShardIndex, CancellationToken cancellationToken = default) =>
        await metaReplica.ProposeAsync(
            new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new MigrationAborted(migrationId, streamName, virtualShardIndex))),
            cancellationToken);

    sealed class AdmissionPauseScope(MigrationAdmissionGate gate, string groupId) : IDisposable {
        public void Dispose() => gate.Resume(groupId);
    }
}
