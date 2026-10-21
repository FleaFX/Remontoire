using Microsoft.Extensions.DependencyInjection;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;

namespace Remontoire.Engine;

/// <summary>
/// Registers the services every Remontoire node shares, embedded or a cluster node — the ones the
/// gRPC services of <c>Remontoire.Server</c> and the health checks resolve. Called by <c>AddEmbedded</c>
/// and <c>AddCluster</c>.
/// </summary>
public static class NodeServices {
    /// <summary>
    /// Registers the shared node services as singletons, and the host's <c>IMeterFactory</c> when it
    /// has none yet.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="hostsMetaGroupReplica">Whether this node hosts a meta-group replica.</param>
    public static void Add(IServiceCollection services, bool hostsMetaGroupReplica) {
        services.AddMetrics();
        services.AddSingleton(new RemontoireNodeInfo(hostsMetaGroupReplica));
        services.AddSingleton<RaftReplicaRegistry>();
        services.AddSingleton<MessagingGroupRegistry>();
        services.AddSingleton<LeaderAddressDirectory>();
        services.AddSingleton<ShardAssignmentTable>();
        services.AddSingleton<MetaLogJournal>();
        services.AddSingleton<MigrationAdmissionGate>();
        services.AddSingleton<ReshardOrchestrator>();
        services.AddSingleton<RemontoireMetrics>();
        services.AddSingleton<RemontoireNode>();
        services.AddSingleton<MessagingOperations>();
    }
}
