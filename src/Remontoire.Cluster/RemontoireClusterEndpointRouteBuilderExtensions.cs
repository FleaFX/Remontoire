using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Remontoire.Cluster.Grpc;
using Remontoire.Raft.Grpc;

namespace Remontoire.Cluster;

/// <summary>
/// Maps the gRPC endpoints of a cluster node registered with
/// <see cref="RemontoireClusterBuilderExtensions.AddCluster"/>. Every method returns the endpoints'
/// convention builder, so the host decides where they listen (<c>RequireHost</c>) and who may call
/// them (<c>RequireAuthorization</c>).
/// </summary>
public static class RemontoireClusterEndpointRouteBuilderExtensions {
    /// <summary>
    /// Maps the endpoints clients use: publish, consume and ack, plus — on a node that hosts a
    /// meta-group replica — the shard-assignment feed clients bootstrap from.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>A builder that applies conventions to every endpoint mapped here.</returns>
    public static IEndpointConventionBuilder MapRemontoire(this IEndpointRouteBuilder endpoints) {
        var client = endpoints.MapGrpcService<RemontoireClientGrpcService>();

        // Without a meta-group replica the journal stays empty: a client bootstrapping from this node
        // would wait forever instead of failing, so the feed isn't mapped here at all.
        if (endpoints.ServiceProvider.GetRequiredService<IOptions<RemontoireClusterOptions>>().Value.MetaGroup is null)
            return client;

        var meta = endpoints.MapGrpcService<ShardAssignmentMetaGrpcService>();
        return new RouteHandlerBuilder([client, meta]); // the framework's public composite of convention builders
    }

    /// <summary>
    /// Maps the Raft endpoint the other nodes of the cluster replicate through.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>A builder that applies conventions to the endpoint mapped here.</returns>
    public static IEndpointConventionBuilder MapRemontoireCluster(this IEndpointRouteBuilder endpoints) =>
        endpoints.MapGrpcService<RaftTransportGrpcService>();

    /// <summary>
    /// Maps the administrative endpoint: streams, policies, resharding and the meta-log audit
    /// trail. Its calls need a meta-group replica on this node; elsewhere they fail with
    /// <c>FailedPrecondition</c>.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>A builder that applies conventions to the endpoint mapped here.</returns>
    public static IEndpointConventionBuilder MapRemontoireAdmin(this IEndpointRouteBuilder endpoints) =>
        endpoints.MapGrpcService<RemontoireAdminGrpcService>();
}
