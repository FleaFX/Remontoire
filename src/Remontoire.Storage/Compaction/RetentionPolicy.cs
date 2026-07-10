namespace Remontoire.Storage;

/// <summary>
/// Size-based emergency-pruning policy — deliberately separate from <see cref="CompactionPolicy"/>:
/// this never merges, never consults the acked watermark, and its own alert is meant to read as
/// categorically more severe than routine compaction/pruning.
/// </summary>
/// <param name="GetMaxTotalBytesPerVirtualShard">
/// The size ceiling <see cref="Compactor.PruneOldestUntilUnderSizeAsync"/> enforces — in practice,
/// per physical group today, not truly per virtual shard (no per-virtual-shard sub-partitioning
/// exists yet in this project). A delegate, re-evaluated on every tick, rather than a fixed value
/// fixed at construction time: the caller may not know the real ceiling yet at the moment a
/// <see cref="ShardLog"/> opens (e.g. its owning stream's sharding assignment hasn't been resolved
/// from a still-catching-up control plane) — a delegate lets that answer change once it becomes
/// known, instead of silently and permanently disabling size-based pruning for that group's whole
/// lifetime. Returning <see langword="null"/> on a given tick disables size-based pruning for that
/// tick only, not forever.
/// </param>
/// <param name="IsAdmissionPaused">
/// Injected the same way a group's local admission pause is consulted everywhere else it might be
/// mid-reshard-pause — this project has no reference to the layer that owns that concept, so the
/// check arrives as a delegate. Gates both this policy's own size-based pruning AND
/// <see cref="ShardLog"/>'s separate, ack-driven retention pass (<see cref="CompactionPolicy.GetAckedLowWatermarkAsync"/>-driven)
/// — one shared admission-pause fact for every periodic pruning path. <see langword="null"/> means
/// never paused.
/// </param>
/// <param name="SizePruneTickInterval">
/// Overrides the size-prune worker's default tick cadence. <see langword="null"/> keeps the
/// production default — this exists purely so tests don't have to wait on that default to observe
/// real, end-to-end pruning behavior.
/// </param>
public sealed record RetentionPolicy(Func<long?> GetMaxTotalBytesPerVirtualShard, Func<bool>? IsAdmissionPaused = null, TimeSpan? SizePruneTickInterval = null);
