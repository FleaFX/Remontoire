using System.Collections.Concurrent;

namespace Remontoire.Sharding;

/// <summary>
/// The materialized, in-memory shard-assignment state — one instance per process (every
/// server node and every client connection each keep their own), rebuilt entirely by
/// replaying a committed log, never itself separately persisted. Reads are lock-free
/// dictionary lookups — no leader redirect, ever.
/// </summary>
public sealed class ShardAssignmentTable {
    readonly ConcurrentDictionary<string, StreamShardingConfig> _streams = new();
    readonly ConcurrentDictionary<string, PhysicalGroupDescriptor> _groups = new();
    readonly ConcurrentDictionary<(string StreamName, int VirtualShardIndex), VirtualShardAssignment> _assignments = new();

    /// <summary>
    /// Looks up a stream's fixed sharding config, if the stream is known.
    /// </summary>
    public bool TryGetStreamConfig(string streamName, out StreamShardingConfig config) => _streams.TryGetValue(streamName, out config);

    /// <summary>
    /// Looks up a physical group's current membership, if the group is known.
    /// </summary>
    public bool TryGetGroup(string groupId, out PhysicalGroupDescriptor group) => _groups.TryGetValue(groupId, out group);

    /// <summary>
    /// Looks up which physical group currently serves a stream's virtual shard.
    /// </summary>
    public bool TryGetAssignment(string streamName, int virtualShardIndex, out VirtualShardAssignment assignment) =>
        _assignments.TryGetValue((streamName, virtualShardIndex), out assignment);

    /// <summary>
    /// Applies one committed <see cref="MetaLogRecord"/>, updating exactly the dictionary its
    /// case concerns. Public so every package that composes its own tailing loop over a meta-log
    /// source can call it directly — but by convention only ever called from that one tailing
    /// loop, never scattered across arbitrary call sites.
    /// </summary>
    /// <remarks>
    /// <see cref="MigrationAborted"/>/<see cref="Cutover"/> are rejected outright (silently — no
    /// table change) when their <see cref="Sharding.MigrationId"/> doesn't match whatever
    /// migration this table currently considers in progress for that (stream, virtual shard)
    /// pair — the same "reject a second, conflicting command instead of applying it" discipline
    /// membership changes already apply, here guarding against a stale or duplicate command
    /// silently corrupting an unrelated, later migration for the same shard.
    /// <see cref="Cutover"/> deliberately keeps <see cref="VirtualShardAssignment.MigrationId"/>
    /// set (only <see cref="VirtualShardAssignment.MigratingToGroupId"/> is cleared) — a
    /// completed migration's id must stay remembered, or a late-arriving duplicate
    /// <see cref="MigrationStarted"/> for that same, already-finished migration would find no
    /// conflicting id in progress and be wrongly re-applied, reverting routing back to the old
    /// group. <see cref="MigrationAborted"/> clears it fully instead: an aborted attempt is
    /// explicitly cancelled, not completed, so its id is free to be reused by a fresh attempt.
    /// </remarks>
    public void Apply(MetaLogRecord record) {
        switch (record) {
            case CreateStream r:
                _streams[r.StreamName] = new StreamShardingConfig(r.StreamName, r.VirtualShardCount, r.RoutingAlgorithm);
                break;

            case RegisterGroup r:
                _groups[r.GroupId] = new PhysicalGroupDescriptor(r.GroupId, r.Members);
                break;

            case MigrationStarted r:
                if (_assignments.TryGetValue((r.StreamName, r.VirtualShardIndex), out var beforeStart)) {
                    // This exact migration already started — whether still in progress (a
                    // harmless, idempotent duplicate) or already cut over (must never be
                    // resurrected) — either way, re-applying it is a no-op.
                    if (beforeStart.MigrationId == r.MigrationId)
                        break;

                    // A different migration is currently active for this shard — reject.
                    if (beforeStart.MigratingToGroupId is not null)
                        break;
                }

                _assignments[(r.StreamName, r.VirtualShardIndex)] =
                    new VirtualShardAssignment(r.StreamName, r.VirtualShardIndex, r.FromGroupId, r.ToGroupId, r.MigrationId);
                break;

            case MigrationAborted r:
                if (_assignments.TryGetValue((r.StreamName, r.VirtualShardIndex), out var beforeAbort) &&
                    beforeAbort.MigrationId == r.MigrationId)
                    _assignments[(r.StreamName, r.VirtualShardIndex)] = beforeAbort with { MigratingToGroupId = null, MigrationId = null };
                break;

            case Cutover r:
                if (_assignments.TryGetValue((r.StreamName, r.VirtualShardIndex), out var beforeCutover) &&
                    beforeCutover.MigrationId == r.MigrationId)
                    _assignments[(r.StreamName, r.VirtualShardIndex)] =
                        new VirtualShardAssignment(r.StreamName, r.VirtualShardIndex, r.ToGroupId, MigrationId: r.MigrationId);
                break;

            case MigrationCompleted:
                // Cleanup of the old group's copy is a disk-reclamation concern, not a routing
                // change — this record carries no table update.
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(record), record, "Unknown MetaLogRecord case.");
        }
    }
}
