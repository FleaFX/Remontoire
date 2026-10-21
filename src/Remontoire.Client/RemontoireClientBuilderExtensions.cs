using Microsoft.Extensions.DependencyInjection;

namespace Remontoire.Client;

/// <summary>
/// Extends <see cref="RemontoireBuilder"/> with the gRPC client.
/// </summary>
public static class RemontoireClientBuilderExtensions {
    /// <summary>
    /// Registers one <see cref="RemontoireConnection"/> as a singleton, resolvable as
    /// <see cref="IRemontoireProducer"/>, as <see cref="IRemontoireConsumer"/> and as itself. The
    /// container disposes it when the host stops.
    /// </summary>
    /// <param name="builder">The builder returned by <c>AddRemontoire()</c>.</param>
    /// <param name="configure">A delegate that configures the <see cref="RemontoireClientOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for further chaining.</returns>
    public static RemontoireBuilder AddClient(this RemontoireBuilder builder, Action<RemontoireClientOptions> configure) {
        var options = new RemontoireClientOptions();
        configure(options);
        builder.Services.AddSingleton(_ => new RemontoireConnection(options));
        builder.Services.AddSingleton<IRemontoireProducer>(sp => sp.GetRequiredService<RemontoireConnection>());
        builder.Services.AddSingleton<IRemontoireConsumer>(sp => sp.GetRequiredService<RemontoireConnection>());
        return builder;
    }
}
