using Microsoft.Extensions.DependencyInjection;

namespace Remontoire;

/// <summary>
/// Entry point of the fluent registration API.
/// </summary>
public static class RemontoireServiceCollectionExtensions {
    /// <summary>
    /// Starts registering Remontoire. Registers nothing itself: the next call decides what this
    /// application is, for example <c>AddCluster</c> for a cluster node or <c>AddClient</c> for a
    /// gRPC client.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    /// <returns>A builder for the next registration.</returns>
    public static RemontoireBuilder AddRemontoire(this IServiceCollection services) => new(services);
}
