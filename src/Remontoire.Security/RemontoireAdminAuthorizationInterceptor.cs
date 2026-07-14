using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Remontoire.Security;

/// <summary>
/// Rejects an RPC on the admin surface unless the caller carries the operator role. Unlike
/// <see cref="RemontoireAuthorizationInterceptor"/>, every admin RPC shares the same,
/// undifferentiated gate — none of them need a resource field off the request, so no per-request-type
/// dispatch is needed here. Registered per-service (<c>AddServiceOptions&lt;RemontoireAdminGrpcService&gt;</c>),
/// after <see cref="RemontoireAuthenticationInterceptor"/> in the same registration chain, so an
/// unauthenticated call is rejected with <see cref="StatusCode.Unauthenticated"/> before it ever
/// reaches this check.
/// </summary>
public sealed class RemontoireAdminAuthorizationInterceptor(RemontoireAuthorizer authorizer) : Interceptor {
    /// <inheritdoc />
    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation) =>
        EnsureOperator(context, () => continuation(request, context));

    /// <inheritdoc />
    public override Task ServerStreamingServerHandler<TRequest, TResponse>(TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context, ServerStreamingServerMethod<TRequest, TResponse> continuation) =>
        EnsureOperator(context, () => continuation(request, responseStream, context));

    TReturn EnsureOperator<TReturn>(ServerCallContext context, Func<TReturn> func) =>
        !authorizer.IsOperator(context.GetHttpContext().User)
            ? throw new RpcException(new Status(StatusCode.PermissionDenied, "Operator role required."))
            : func();
}
