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

    /// <summary>
    /// Declares a stream this application owns, ensured with <see cref="IRemontoireAdmin.EnsureStreamAsync"/>
    /// while the host starts: created when missing, updated when the options changed. Works with
    /// <c>AddEmbedded</c> and <c>AddClient</c>; a cluster node refuses to start with declarations.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="configure">Configures the <see cref="StreamOptions"/>; <see langword="null"/> keeps the defaults.</param>
    /// <returns>This builder, for further chaining.</returns>
    public RemontoireBuilder AddStream(string streamName, Action<StreamOptions>? configure = null) {
        var options = new StreamOptions();
        configure?.Invoke(options);
        Services.AddSingleton<RemontoireDeclaration>(new StreamDeclaration(streamName, options));
        return this;
    }

    /// <summary>
    /// Declares a consumer group this application consumes with, ensured with
    /// <see cref="IRemontoireAdmin.EnsureConsumerGroupAsync"/> while the host starts, after the
    /// streams. Works with <c>AddEmbedded</c> and <c>AddClient</c>; a cluster node refuses to start
    /// with declarations.
    /// </summary>
    /// <param name="streamName">The stream the group consumes.</param>
    /// <param name="consumerGroup">The consumer group.</param>
    /// <param name="configure">Configures the <see cref="ConsumerGroupOptions"/>; <see langword="null"/> keeps the defaults.</param>
    /// <returns>This builder, for further chaining.</returns>
    public RemontoireBuilder AddConsumerGroup(string streamName, string consumerGroup, Action<ConsumerGroupOptions>? configure = null) {
        var options = new ConsumerGroupOptions();
        configure?.Invoke(options);
        Services.AddSingleton<RemontoireDeclaration>(new ConsumerGroupDeclaration(streamName, consumerGroup, options));
        return this;
    }
}
