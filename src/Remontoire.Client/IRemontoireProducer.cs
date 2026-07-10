namespace Remontoire.Client;

/// <summary>
/// Publishes messages to a Remontoire stream.
/// </summary>
public interface IRemontoireProducer {
    /// <summary>
    /// Publishes one message to <paramref name="streamName"/>, keyed by <paramref name="partitionKey"/>.
    /// Completes only once the message is quorum-committed — never on mere local acceptance.
    /// </summary>
    Task<PublishResult> PublishAsync(
        string streamName,
        string partitionKey,
        ReadOnlyMemory<byte> payload,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default
    );
}
