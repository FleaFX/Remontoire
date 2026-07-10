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
                // A different migration already in progress for this exact shard is rejected —
                // re-proposing the SAME migration (the same id) is a harmless, idempotent no-op.
                if (_assignments.TryGetValue((r.StreamName, r.VirtualShardIndex), out var beforeStart) &&
                    beforeStart.MigrationId is { } inProgress && inProgress != r.MigrationId)
                    break;

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
                        new VirtualShardAssignment(r.StreamName, r.VirtualShardIndex, r.ToGroupId);
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
