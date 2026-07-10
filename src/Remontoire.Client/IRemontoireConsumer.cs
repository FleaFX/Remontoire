namespace Remontoire.Client;

/// <summary>
/// Consumes and acknowledges messages from a Remontoire stream, as one named consumer group.
/// </summary>
public interface IRemontoireConsumer {
    /// <summary>
    /// Streams every not-yet-acked message on <paramref name="streamName"/> for <paramref name="consumerGroup"/>,
    /// live-tailing as new messages arrive. The consumer controls its own read pace.
    /// </summary>
    IAsyncEnumerable<RemontoireMessage> ConsumeAsync(string streamName, string consumerGroup, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges one message for <paramref name="consumerGroup"/>. Idempotent — acking an
    /// already-acked offset is a safe no-op.
    /// </summary>
    Task AckAsync(string streamName, string consumerGroup, int shardId, long offset, CancellationToken cancellationToken = default);
}
