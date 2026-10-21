using Microsoft.Extensions.DependencyInjection;

namespace Remontoire.Server;

/// <summary>
/// Extends <see cref="RemontoireBuilder"/> with the gRPC endpoints of a node.
/// </summary>
public static class RemontoireServerBuilderExtensions {
    /// <summary>
    /// Registers the gRPC services the methods of <see cref="RemontoireServerEndpointRouteBuilderExtensions"/>
    /// map, on top of a node registered with <c>AddEmbedded</c> or <c>AddCluster</c>.
    /// </summary>
    /// <param name="builder">The builder returned by <c>AddRemontoire()</c>.</param>
    /// <returns>The same <paramref name="builder"/> for further chaining.</returns>
    public static RemontoireBuilder AddServer(this RemontoireBuilder builder) {
        builder.Services.AddGrpc();
        return builder;
    }
}
