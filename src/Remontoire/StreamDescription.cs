namespace Remontoire;

/// <summary>
/// A stream's configuration as <see cref="IRemontoireAdmin.GetStreamAsync"/> and
/// <see cref="IRemontoireAdmin.ListStreamsAsync"/> report it.
/// </summary>
/// <param name="Name">The stream's name.</param>
/// <param name="VirtualShardCount">The number of virtual shards, fixed at creation.</param>
/// <param name="MaxRetention">See <see cref="StreamOptions.MaxRetention"/>.</param>
/// <param name="MaxSizeBytesPerVirtualShard">See <see cref="StreamOptions.MaxSizeBytesPerVirtualShard"/>.</param>
/// <param name="CheckpointInterval">See <see cref="StreamOptions.CheckpointInterval"/>.</param>
/// <param name="CheckpointOffsetCount">See <see cref="StreamOptions.CheckpointOffsetCount"/>.</param>
/// <param name="ConsumerGroups">The consumer groups declared on the stream, in ordinal name order.</param>
public sealed record StreamDescription(
    string Name,
    int VirtualShardCount,
    TimeSpan MaxRetention,
    long? MaxSizeBytesPerVirtualShard,
    TimeSpan? CheckpointInterval,
    int? CheckpointOffsetCount,
    IReadOnlyList<ConsumerGroupDescription> ConsumerGroups
);

/// <summary>
/// A consumer group's configuration, as part of a <see cref="StreamDescription"/>.
/// </summary>
/// <param name="Name">The consumer group's name.</param>
/// <param name="AckMode">See <see cref="ConsumerGroupOptions.AckMode"/>.</param>
/// <param name="Mandatory">See <see cref="ConsumerGroupOptions.Mandatory"/>.</param>
/// <param name="MaxRetries">See <see cref="ConsumerGroupOptions.MaxRetries"/>.</param>
/// <param name="InitialRetryDelay">See <see cref="ConsumerGroupOptions.InitialRetryDelay"/>.</param>
/// <param name="MaxRetryDelay">See <see cref="ConsumerGroupOptions.MaxRetryDelay"/>.</param>
public sealed record ConsumerGroupDescription(
    string Name,
    AckMode AckMode,
    bool Mandatory,
    int MaxRetries,
    TimeSpan InitialRetryDelay,
    TimeSpan MaxRetryDelay
);
