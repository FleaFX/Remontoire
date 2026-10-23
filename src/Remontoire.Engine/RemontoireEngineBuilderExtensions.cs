using Microsoft.Extensions.DependencyInjection;

namespace Remontoire.Engine;

/// <summary>
/// Extends <see cref="RemontoireBuilder"/> with the embedded engine.
/// </summary>
public static class RemontoireEngineBuilderExtensions {
    /// <summary>
    /// Registers Remontoire embedded in this process — one Raft group without peers per stream, and
    /// one for the meta log, without network — started and stopped with the host. Registers
    /// <see cref="IRemontoireProducer"/>, <see cref="IRemontoireConsumer"/> and
    /// <see cref="IRemontoireAdmin"/>, and ensures the declarations of <c>AddStream</c> and
    /// <c>AddConsumerGroup</c> once the engine has started. Add <c>AddServer()</c> from
    /// <c>Remontoire.Server</c> to make it reachable over gRPC as well.
    /// </summary>
    /// <param name="builder">The builder returned by <c>AddRemontoire()</c>.</param>
    /// <param name="configure">A delegate that configures the <see cref="RemontoireEngineOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for further chaining.</returns>
    /// <exception cref="ArgumentException"><see cref="RemontoireEngineOptions.DataDirectory"/> is not set.</exception>
    public static RemontoireBuilder AddEmbedded(this RemontoireBuilder builder, Action<RemontoireEngineOptions> configure) {
        var options = new RemontoireEngineOptions();
        configure(options);
        if (string.IsNullOrWhiteSpace(options.DataDirectory))
            throw new ArgumentException($"Configure {nameof(RemontoireEngineOptions.DataDirectory)}.", nameof(configure));

        var services = builder.Services;
        NodeServices.Add(services, hostsMetaGroupReplica: true, ownGroupMember: EmbeddedHostedService.Member(options));
        services.AddSingleton(options);
        services.AddSingleton<EmbeddedRemontoire>();
        services.AddSingleton<IRemontoireProducer>(provider => provider.GetRequiredService<EmbeddedRemontoire>());
        services.AddSingleton<IRemontoireConsumer>(provider => provider.GetRequiredService<EmbeddedRemontoire>());
        services.AddHostedService<EmbeddedHostedService>();
        // After the engine: the declarations need it started.
        services.AddHostedService<RemontoireDeclarationsHostedService>();
        return builder;
    }
}
