using Grpc.Core;
using Grpc.Core.Interceptors;
using Remontoire.Client.V1;

namespace Remontoire.Security;

/// <summary>
/// Resource-specific authorization for the client gRPC service's RPCs — kept out of that
/// service's own method bodies entirely, the same separation-of-concerns <c>[Authorize]</c> gives
/// an ordinary ASP.NET Core controller, adapted for gRPC (where the declarative attribute itself
/// has no way to see a strongly-typed request's own fields, e.g. <c>StreamName</c>/<c>ConsumerGroup</c>,
/// without a resource-based check — an interceptor is the gRPC-native place for that). Registered
/// per-service, right after <see cref="RemontoireAuthenticationInterceptor"/> — a host that never
/// registers either interceptor (every pre-fase-8 test harness) never runs this check at all,
/// exactly the same opt-in shape the authentication interceptor already has.
/// </summary>
public sealed class RemontoireAuthorizationInterceptor(RemontoireAuthorizer authorizer) : Interceptor {
    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation) {
        EnsureAuthorized(request, context);
        return continuation(request, context);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation) {
        EnsureAuthorized(request, context);
        return continuation(request, responseStream, context);
    }

    // Checked before the client gRPC service ever sees the request, not after — an unauthorized
    // caller must not be able to distinguish "no such stream" (NotFound) from "stream exists, you
    // may not touch it" (PermissionDenied) by which error comes back, in a multi-tenant deployment.
    void EnsureAuthorized<TRequest>(TRequest request, ServerCallContext context) {
        var user = context.GetHttpContext().User;
        switch (request) {
            case PublishRequest publish when !authorizer.CanProduce(user, publish.StreamName):
                throw new RpcException(new Status(StatusCode.PermissionDenied, $"Not authorized to produce on '{publish.StreamName}'."));

            case AckRequest ack when !authorizer.CanConsume(user, ack.StreamName, ack.ConsumerGroup):
                throw new RpcException(new Status(StatusCode.PermissionDenied, $"Not authorized to consume/ack as '{ack.ConsumerGroup}' on '{ack.StreamName}'."));

            case ConsumeRequest consume when !authorizer.CanConsume(user, consume.StreamName, consume.ConsumerGroup):
                throw new RpcException(new Status(StatusCode.PermissionDenied, $"Not authorized to consume/ack as '{consume.ConsumerGroup}' on '{consume.StreamName}'."));
        }
    }
}
