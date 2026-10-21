namespace Remontoire;

/// <summary>
/// Administers Remontoire's streams.
/// </summary>
public interface IRemontoireAdmin {
    /// <summary>
    /// Creates <paramref name="streamName"/>, or does nothing when it already exists. Completes once
    /// the stream accepts messages.
    /// </summary>
    /// <param name="streamName">The name of the stream.</param>
    /// <param name="cancellationToken">Cancels the wait; a creation already proposed still completes.</param>
    Task CreateStreamAsync(string streamName, CancellationToken cancellationToken = default);
}
