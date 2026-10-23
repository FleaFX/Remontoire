using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Remontoire.Engine;

namespace Remontoire.Server;

/// <summary>
/// Maps the gRPC endpoints of a node registered with <c>AddEmbedded</c> or <c>AddCluster</c>, and
/// <see cref="RemontoireServerBuilderExtensions.AddServer"/>. Every method returns the endpoints'
/// convention builder, so the host decides where they listen (<c>RequireHost</c>) and who may call
/// them (<c>RequireAuthorization</c>).
/// </summary>
public static class RemontoireServerEndpointRouteBuilderExtensions {
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
        if (!endpoints.ServiceProvider.GetRequiredService<RemontoireNodeInfo>().HostsMetaGroupReplica)
            return client;

        var meta = endpoints.MapGrpcService<ShardAssignmentMetaGrpcService>();
        return new RouteHandlerBuilder([client, meta]); // the framework's public composite of convention builders
    }

    /// <summary>
    /// Maps the administrative endpoint: declaring streams and consumer groups, resharding and the
    /// meta-log audit trail. Its calls need a meta-group replica on this node; elsewhere they fail
    /// with <c>FailedPrecondition</c>. <c>Remontoire.Client</c> declares through it, with the client's
    /// token hook.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>A builder that applies conventions to the endpoint mapped here.</returns>
    public static IEndpointConventionBuilder MapRemontoireAdmin(this IEndpointRouteBuilder endpoints) =>
        endpoints.MapGrpcService<RemontoireAdminGrpcService>();
}
