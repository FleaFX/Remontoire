namespace Remontoire;

/// <summary>
/// One message yielded by <see cref="IRemontoireConsumer.ConsumeAsync"/>.
/// </summary>
/// <param name="ShardId">The virtual shard this message came from. Always zero until sharding exists.</param>
/// <param name="Offset">The message's consumer-visible offset within its shard — pass this to <see cref="IRemontoireConsumer.AckAsync"/>.</param>
/// <param name="PartitionKey">The key the message was published with.</param>
/// <param name="Payload">The opaque message payload.</param>
/// <param name="Headers">Free-form key/value metadata attached to the message.</param>
/// <param name="IngestedAt">The moment the leader accepted the message.</param>
/// <param name="Attempt">Which delivery attempt this is for the consumer group: 1 the first time, one more after every <see cref="IRemontoireConsumer.NackAsync"/> of it.</param>
public sealed record RemontoireMessage(
    int ShardId,
    long Offset,
    string PartitionKey,
    ReadOnlyMemory<byte> Payload,
    IReadOnlyDictionary<string, string> Headers,
    DateTimeOffset IngestedAt,
    int Attempt
);
