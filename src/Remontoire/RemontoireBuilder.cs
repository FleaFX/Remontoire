using Microsoft.Extensions.DependencyInjection;

namespace Remontoire;

/// <summary>
/// Returned by <see cref="RemontoireServiceCollectionExtensions.AddRemontoire"/>; the hosting
/// packages extend it, for example with <c>AddCluster</c> or <c>AddClient</c>.
/// </summary>
/// <param name="services">The service collection Remontoire registers into.</param>
public sealed class RemontoireBuilder(IServiceCollection services) {
    /// <summary>
    /// The service collection Remontoire registers into.
    /// </summary>
    public IServiceCollection Services { get; } = services;
}
