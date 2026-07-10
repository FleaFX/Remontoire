namespace Remontoire.Client;

/// <summary>
/// What a successful <see cref="IRemontoireProducer.PublishAsync"/> call reports back.
/// </summary>
/// <param name="ShardId">The virtual shard the message landed on. Always zero until sharding exists.</param>
/// <param name="Offset">The consumer-visible, monotonically increasing offset the leader assigned.</param>
/// <param name="IngestedAt">The moment the leader accepted the message — its own timestamp, not a client-side approximation.</param>
public sealed record PublishResult(int ShardId, long Offset, DateTimeOffset IngestedAt);
