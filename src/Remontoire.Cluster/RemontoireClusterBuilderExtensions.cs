using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Remontoire.Engine;

namespace Remontoire.Cluster;

/// <summary>
/// Extends <see cref="RemontoireBuilder"/> with a Raft cluster node.
/// </summary>
public static class RemontoireClusterBuilderExtensions {
    /// <summary>
    /// Registers a cluster node: every data group and the meta-group replica configured in
    /// <see cref="RemontoireClusterOptions"/>, started and stopped with the host, and the Raft
    /// endpoint <see cref="RemontoireClusterEndpointRouteBuilderExtensions.MapRemontoireCluster"/>
    /// maps. Add <c>AddServer()</c> from <c>Remontoire.Server</c> for the client and admin endpoints.
    /// </summary>
    /// <param name="builder">The builder returned by <c>AddRemontoire()</c>.</param>
    /// <param name="configure">A delegate that configures the <see cref="RemontoireClusterOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for further chaining.</returns>
    /// <exception cref="ArgumentException">The node neither hosts a meta-group replica nor knows a meta-group seed address.</exception>
    public static RemontoireBuilder AddCluster(this RemontoireBuilder builder, Action<RemontoireClusterOptions> configure) {
        var options = new RemontoireClusterOptions();
        configure(options);

        // A node learns which group serves which shard only from the meta group: without a replica
        // of its own or a seed to watch, it would start but never serve anything.
        if (options.MetaGroup is null && options.MetaGroupSeedAddresses.Count == 0)
            throw new ArgumentException(
                $"Configure {nameof(RemontoireClusterOptions.MetaGroup)} or at least one of {nameof(RemontoireClusterOptions.MetaGroupSeedAddresses)}.",
                nameof(configure));

        var services = builder.Services;
        NodeServices.Add(services, hostsMetaGroupReplica: options.MetaGroup is not null);
        services.AddSingleton(Options.Create(options));
        services.AddHostedService<RaftReplicaHostedService>();
        services.AddGrpc();
        return builder;
    }
}
