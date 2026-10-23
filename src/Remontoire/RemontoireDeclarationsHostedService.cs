using System.ComponentModel;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Remontoire;

/// <summary>
/// Ensures every <see cref="RemontoireDeclaration"/> while the host starts: the streams first, then
/// the consumer groups, so a hosted service registered after it finds them. Waits, retrying every
/// second, while Remontoire cannot be reached; any other failure fails the start. Registered by the
/// hosting packages that support declarations.
/// </summary>
/// <param name="admin">Ensures each declaration.</param>
/// <param name="declarations">Every declaration, in registration order.</param>
/// <param name="logger">Logs each retry.</param>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class RemontoireDeclarationsHostedService(
    IRemontoireAdmin admin,
    IEnumerable<RemontoireDeclaration> declarations,
    ILogger<RemontoireDeclarationsHostedService> logger
) : IHostedService {
    static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(1);

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken) {
        var all = declarations.ToArray();
        foreach (var stream in all.OfType<StreamDeclaration>())
            await RetryWhileUnavailableAsync(ct => admin.EnsureStreamAsync(stream.StreamName, stream.Options, ct), cancellationToken);
        foreach (var group in all.OfType<ConsumerGroupDeclaration>())
            await RetryWhileUnavailableAsync(ct => admin.EnsureConsumerGroupAsync(group.StreamName, group.ConsumerGroup, group.Options, ct), cancellationToken);
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    async Task RetryWhileUnavailableAsync(Func<CancellationToken, Task> ensure, CancellationToken cancellationToken) {
        while (true) {
            try {
                await ensure(cancellationToken);
                return;
            } catch (RemontoireUnavailableException ex) {
                logger.LogWarning(ex, "Remontoire cannot be reached yet; retrying the declarations in {RetryDelay}.", RetryDelay);
                await Task.Delay(RetryDelay, cancellationToken);
            }
        }
    }
}
