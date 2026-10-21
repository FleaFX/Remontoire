namespace Remontoire.Sharding;

/// <summary>
/// One stream's retention/checkpoint knobs — everything about it an operator may adjust after the
/// stream exists. Deliberately a separate type from <see cref="StreamShardingConfig"/>, never
/// merged into it: that type is immutable by contract, fixed once at stream creation and never
/// changed afterward, while every field here is explicitly meant to be adjustable later. Mirrors
/// how <see cref="VirtualShardAssignment"/> is already a separate, independently-mutable sibling
/// of <see cref="StreamShardingConfig"/> in the same table, not a field bolted onto it.
/// </summary>
/// <param name="AuditRetention">Kept this long after every mandatory group has acked, purely for troubleshooting.</param>
/// <param name="MaxRetention">A hard ceiling from ingest, regardless of ack status — the escape hatch against a permanently stuck mandatory group.</param>
/// <param name="MaxSizeBytesPerVirtualShard">The emergency size ceiling. <see langword="null"/> disables size-based emergency pruning.</param>
/// <param name="CheckpointInterval">Checkpoint-mode's time trigger. <see langword="null"/> disables it.</param>
/// <param name="CheckpointOffsetCount">Checkpoint-mode's offset-count trigger. <see langword="null"/> disables it.</param>
public readonly record struct StreamRetentionPolicy(
    TimeSpan AuditRetention,
    TimeSpan MaxRetention,
    long? MaxSizeBytesPerVirtualShard,
    TimeSpan? CheckpointInterval,
    int? CheckpointOffsetCount
);
