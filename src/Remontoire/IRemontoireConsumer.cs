namespace Remontoire;

/// <summary>
/// Consumes and acknowledges messages from a Remontoire stream, as one named consumer group.
/// </summary>
/// <remarks>
/// A consumer group is one cursor over the stream, with one active consumer: messages arrive in
/// offset order, and a nacked message holds back every later one until it is acked or parked.
/// </remarks>
public interface IRemontoireConsumer {
    /// <summary>
    /// Streams every not-yet-acked message on <paramref name="streamName"/> for <paramref name="consumerGroup"/>,
    /// live-tailing as new messages arrive. The consumer controls its own read pace. After a
    /// <see cref="NackAsync"/>, the stream offers the nacked message again once its delay has passed,
    /// before any later message.
    /// </summary>
    IAsyncEnumerable<RemontoireMessage> ConsumeAsync(string streamName, string consumerGroup, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges one message for <paramref name="consumerGroup"/>. Idempotent — acking an
    /// already-acked offset is a safe no-op.
    /// </summary>
    Task AckAsync(string streamName, string consumerGroup, int shardId, long offset, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports that handling one message failed for <paramref name="consumerGroup"/>. The message is
    /// offered again, with the next <see cref="RemontoireMessage.Attempt"/>, after <paramref name="delay"/>
    /// or else the group's backoff; until then no later message arrives. Once the group's
    /// <see cref="ConsumerGroupOptions.MaxRetries"/> are used up, the message is parked instead: it
    /// counts as handled, and the group continues with the next one. A nack for an offset that is
    /// already acked or parked does nothing. Nack through the same consumer that consumes.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <param name="shardId">The message's <see cref="RemontoireMessage.ShardId"/>.</param>
    /// <param name="offset">The message's <see cref="RemontoireMessage.Offset"/>.</param>
    /// <param name="reason">Why handling failed, kept with a parked message; only the first 1024 characters are kept.</param>
    /// <param name="delay">How long to wait before the next attempt; <see langword="null"/> for the group's backoff.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    Task NackAsync(
        string streamName,
        string consumerGroup,
        int shardId,
        long offset,
        string? reason = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default
    );
}
