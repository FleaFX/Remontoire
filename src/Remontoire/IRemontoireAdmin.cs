namespace Remontoire;

/// <summary>
/// Declares and describes Remontoire's streams and consumer groups. A declaration is idempotent:
/// it creates what is missing, applies what changed, and does nothing when everything matches.
/// </summary>
public interface IRemontoireAdmin {
    /// <summary>
    /// Creates <paramref name="streamName"/> with <paramref name="options"/>, or applies
    /// <paramref name="options"/> to the existing stream. Completes once this node or connection sees
    /// the result; embedded, once the stream accepts messages.
    /// </summary>
    /// <param name="streamName">1 to 100 letters, digits, '.', '_' or '-', starting with a letter or digit.</param>
    /// <param name="options">The stream's configuration; <see langword="null"/> for the defaults.</param>
    /// <param name="cancellationToken">Cancels the wait; a declaration already proposed still completes.</param>
    /// <exception cref="ArgumentException">The name or an option is not valid.</exception>
    /// <exception cref="RemontoireConflictException">The stream exists with a configuration a declaration cannot change.</exception>
    /// <exception cref="RemontoireUnavailableException">Remontoire could not be reached in time; retrying may succeed.</exception>
    Task EnsureStreamAsync(string streamName, StreamOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Declares <paramref name="consumerGroup"/> on <paramref name="streamName"/> with
    /// <paramref name="options"/>, or applies <paramref name="options"/> to the existing group. Only a
    /// declared group can consume and ack. The stream need not exist yet. Completes once this node or
    /// connection sees the result.
    /// </summary>
    /// <param name="streamName">The stream the group consumes.</param>
    /// <param name="consumerGroup">Same rules as a stream name.</param>
    /// <param name="options">The group's configuration; <see langword="null"/> for the defaults.</param>
    /// <param name="cancellationToken">Cancels the wait; a declaration already proposed still completes.</param>
    /// <exception cref="ArgumentException">A name or an option is not valid.</exception>
    /// <exception cref="RemontoireUnavailableException">Remontoire could not be reached in time; retrying may succeed.</exception>
    Task EnsureConsumerGroupAsync(string streamName, string consumerGroup, ConsumerGroupOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Describes <paramref name="streamName"/> as this node or connection knows it.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="cancellationToken">Unused; for symmetry.</param>
    /// <returns>The description, or <see langword="null"/> for an unknown stream.</returns>
    Task<StreamDescription?> GetStreamAsync(string streamName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Describes every stream this node or connection knows, in ordinal name order.
    /// </summary>
    /// <param name="cancellationToken">Unused; for symmetry.</param>
    /// <returns>The descriptions.</returns>
    Task<IReadOnlyList<StreamDescription>> ListStreamsAsync(CancellationToken cancellationToken = default);
}
