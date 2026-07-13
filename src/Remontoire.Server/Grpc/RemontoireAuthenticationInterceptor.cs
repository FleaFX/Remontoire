using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Remontoire.Server.Grpc;

/// <summary>
/// Rejects an RPC that carries no valid, authenticated identity. The actual bearer-token
/// validation (signature, expiry, issuer, audience) already happened in ASP.NET Core's own
/// authentication middleware before gRPC endpoint dispatch — and therefore this interceptor —
/// ever runs; this class only checks whether that produced an authenticated identity. Registered
/// per-service (<c>AddServiceOptions&lt;RemontoireClientGrpcService&gt;</c>), never globally —
/// <see cref="Remontoire.Raft.Grpc.RaftTransportGrpcService"/>'s node-to-node traffic never
/// carries a bearer token and must never be required to.
/// </summary>
public sealed class RemontoireAuthenticationInterceptor : Interceptor {
    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation) {
        EnsureAuthenticated(context);
        return continuation(request, context);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation) {
        EnsureAuthenticated(context);
        return continuation(request, responseStream, context);
    }

    static void EnsureAuthenticated(ServerCallContext context) {
        if (context.GetHttpContext().User.Identity?.IsAuthenticated != true)
            throw new RpcException(new Status(StatusCode.Unauthenticated, "A valid bearer token is required."));
    }
}
