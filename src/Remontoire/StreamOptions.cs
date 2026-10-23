namespace Remontoire;

/// <summary>
/// The configuration a stream is declared with, through <see cref="IRemontoireAdmin.EnsureStreamAsync"/>
/// or <see cref="RemontoireBuilder.AddStream"/>. Every property can change later: declaring the
/// stream again with other values applies them.
/// </summary>
public sealed class StreamOptions {
    /// <summary>
    /// How long a message is kept at most, counted from its publication. A message that a mandatory
    /// consumer group has not acked by then is forwarded to the stream <c>&lt;stream&gt;.__deadletter__</c>
    /// when that stream exists, and kept otherwise. Defaults to 30 days.
    /// </summary>
    public TimeSpan MaxRetention { get; set; } = TimeSpan.FromDays(30);

    /// <summary>
    /// The emergency size limit per virtual shard, in bytes: beyond it the oldest segments are
    /// pruned, acked or not. <see langword="null"/> (the default) sets no limit.
    /// </summary>
    public long? MaxSizeBytesPerVirtualShard { get; set; }

    /// <summary>
    /// How often the acks of a consumer group in <see cref="AckMode.Checkpoint"/> are replicated at
    /// the latest. <see langword="null"/> (the default) disables this trigger; with
    /// <see cref="CheckpointOffsetCount"/> also <see langword="null"/>, they are replicated every 30 seconds.
    /// </summary>
    public TimeSpan? CheckpointInterval { get; set; }

    /// <summary>
    /// After how many acked offsets the acks of a consumer group in <see cref="AckMode.Checkpoint"/>
    /// are replicated at the latest. <see langword="null"/> (the default) disables this trigger.
    /// </summary>
    public int? CheckpointOffsetCount { get; set; }
}
