using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;

namespace Remontoire.Cluster;

/// <summary>
/// Extends <see cref="RemontoireBuilder"/> with a Raft cluster node.
/// </summary>
public static class RemontoireClusterBuilderExtensions {
    /// <summary>
    /// Registers a cluster node: every data group and the meta-group replica configured in
    /// <see cref="RemontoireClusterOptions"/>, started and stopped with the host, and the gRPC
    /// services the methods of <see cref="RemontoireClusterEndpointRouteBuilderExtensions"/> map.
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
        services.AddSingleton(Options.Create(options));
        services.AddSingleton<RaftReplicaRegistry>();
        services.AddSingleton<MessagingGroupRegistry>();
        services.AddSingleton<LeaderAddressDirectory>();
        services.AddSingleton<ShardAssignmentTable>();
        services.AddSingleton<MetaLogJournal>();
        services.AddSingleton<MigrationAdmissionGate>();
        services.AddSingleton<ReshardOrchestrator>();
        services.AddHostedService<RaftReplicaHostedService>();
        services.AddGrpc();
        return builder;
    }
}
