using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Remontoire.Security;

/// <summary>
/// Defense-in-depth only — the real trust decision for node-to-node traffic is Kestrel's own
/// <c>ClientCertificateMode.RequireCertificate</c> on the peer port (§5.3), enforced at the TLS
/// handshake before this interceptor, or any other application code, ever runs. This class exists
/// to catch a future routing/wiring mistake (e.g. a peer RPC accidentally reachable on the client
/// port) even after that handshake-level decision was already made correctly — it asserts the
/// connection actually carries a peer client certificate, nothing more. Registered per-service on
/// the Raft transport service only, mirroring <see cref="RemontoireAuthenticationInterceptor"/>'s
/// own opt-in shape on the client-facing service.
/// </summary>
public sealed class PeerCertificateRequiredInterceptor : Interceptor {
    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation) {
        if (context.GetHttpContext().Connection.ClientCertificate is null)
            throw new RpcException(new Status(StatusCode.Unauthenticated, "A peer client certificate is required."));

        return continuation(request, context);
    }
}
