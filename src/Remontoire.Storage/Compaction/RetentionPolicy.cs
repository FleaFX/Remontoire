namespace Remontoire.Storage;

/// <summary>
/// Size-based emergency-pruning policy — deliberately separate from <see cref="CompactionPolicy"/>:
/// this never merges, never consults the acked watermark, and its own alert is meant to read as
/// categorically more severe than routine compaction/pruning.
/// </summary>
/// <param name="MaxTotalBytesPerVirtualShard">
/// The size ceiling <see cref="Compactor.PruneOldestUntilUnderSizeAsync"/> enforces — in practice,
/// per physical group today, not truly per virtual shard (no per-virtual-shard sub-partitioning
/// exists yet in this project). <see langword="null"/> disables size-based emergency pruning
/// entirely.
/// </param>
/// <param name="IsAdmissionPaused">
/// Injected the same way a group's local admission pause is consulted everywhere else it might be
/// mid-reshard-pause — this project has no reference to the layer that owns that concept, so the
/// check arrives as a delegate. Gates both this policy's own size-based pruning AND
/// <see cref="ShardLog"/>'s separate, ack-driven retention pass (<see cref="CompactionPolicy.GetAckedLowWatermarkAsync"/>-driven)
/// — one shared admission-pause fact for every periodic pruning path. <see langword="null"/> means
/// never paused.
/// </param>
public sealed record RetentionPolicy(long? MaxTotalBytesPerVirtualShard, Func<bool>? IsAdmissionPaused = null);
