namespace Remontoire.Sharding;

/// <summary>
/// How a consumer group's acknowledgments replicate. <see cref="Strict"/>: every ack is its own
/// Raft-committed record — the only mode before this type existed, unchanged.
/// <see cref="Checkpoint"/>: acks apply immediately, locally, on the leader; only a periodic
/// low-watermark checkpoint is Raft-committed — cheaper, at the cost of up to one checkpoint
/// interval's worth of extra redelivery on failover, still within the at-least-once guarantee.
/// </summary>
public enum AckMode : byte {
    /// <summary>
    /// Every ack is its own Raft-committed record.
    /// </summary>
    Strict = 0,

    /// <summary>
    /// Acks apply locally on the leader; only a periodic low-watermark is replicated.
    /// </summary>
    Checkpoint = 1,
}
