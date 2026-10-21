using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Remontoire.Raft.Grpc;

namespace Remontoire.Cluster;

/// <summary>
/// Maps the Raft endpoint of a cluster node registered with
/// <see cref="RemontoireClusterBuilderExtensions.AddCluster"/>. The client and admin endpoints come
/// from <c>Remontoire.Server</c>. The method returns the endpoint's convention builder, so the host
/// decides where it listens (<c>RequireHost</c>).
/// </summary>
public static class RemontoireClusterEndpointRouteBuilderExtensions {
    /// <summary>
    /// Maps the Raft endpoint the other nodes of the cluster replicate through.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>A builder that applies conventions to the endpoint mapped here.</returns>
    public static IEndpointConventionBuilder MapRemontoireCluster(this IEndpointRouteBuilder endpoints) =>
        endpoints.MapGrpcService<RaftTransportGrpcService>();
}
