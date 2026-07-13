using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Remontoire.Raft.V1;
using Remontoire.Security;

namespace Remontoire.Raft.Grpc;

/// <summary>
/// Routes Raft RPCs over gRPC. One channel per peer node, shared by every replica this node
/// hosts — never per group (the day-one multi-group reservation). Every call carries the
/// constructor-provided RPC timeout as its deadline, and every channel pings its peer even while
/// idle — the two hardening lessons from a sister project's real production
/// incident: an RPC that can hang forever, or a "healthy looking" connection that infrastructure
/// silently dropped, each stall an election or a leader without the caller ever finding out.
/// </summary>
public sealed class RaftGrpcTransport : IRaftTransport, IDisposable {
    readonly Dictionary<string, GrpcChannel> _channels;
    readonly Dictionary<string, RaftTransport.RaftTransportClient> _clients;
    readonly TimeSpan _rpcTimeout;

    /// <param name="peers">The reachable peer nodes.</param>
    /// <param name="rpcTimeout">Deadline applied to every outbound RPC, no exceptions.</param>
    /// <param name="mtls">
    /// Cluster mTLS configuration — either real certificates to present/validate, or an explicit
    /// opt-out into insecure transport (<see cref="ClusterMtlsOptions.AllowInsecureTransport"/>,
    /// dev/test only). Required, no default: a caller must make one of the two choices explicitly,
    /// there is no silent third option.
    /// </param>
    /// <param name="expectedCertificateSubjects">
    /// Each peer's expected certificate subject, keyed by <see cref="RaftGroupMember.NodeId"/> —
    /// dialing peer X checks precisely X's own expected subject. <see langword="null"/> or a
    /// missing entry means no additional subject check beyond the CA chain itself.
    /// </param>
    /// <param name="interceptors">Client interceptors applied to every channel — empty by default.</param>
    /// <param name="logger">
    /// Used to log, at <see cref="LogLevel.Critical"/>, every time this transport runs unencrypted
    /// because <see cref="ClusterMtlsOptions.AllowInsecureTransport"/> was explicitly set — never
    /// silent, never once-only.
    /// </param>
    public RaftGrpcTransport(
        IReadOnlyList<RaftGroupMember> peers, TimeSpan rpcTimeout, ClusterMtlsOptions mtls,
        IReadOnlyDictionary<string, string>? expectedCertificateSubjects = null,
        IReadOnlyList<Interceptor>? interceptors = null, ILogger? logger = null) {
        _rpcTimeout = rpcTimeout;
        interceptors ??= [];

        ClusterMtlsCredentials? credentials = null;
        if (mtls.AllowInsecureTransport) {
            // Without mTLS, peer addresses are plain http:// — .NET's HTTP/2 client otherwise
            // refuses cleartext HTTP/2 outright. Set per-instance (never as an unconditional
            // static switch) and logged loudly every time, so an accidentally-insecure deployment
            // fails loud rather than silently succeeding.
            logger?.LogCritical("AllowInsecureTransport is set — Raft peer traffic runs unencrypted, without mTLS. Never use this outside development/test.");
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        } else {
            credentials = ClusterMtlsCredentialsLoader.Load(mtls);
        }

        _channels = new Dictionary<string, GrpcChannel>(peers.Count);
        _clients = new Dictionary<string, RaftTransport.RaftTransportClient>(peers.Count);

        // A later peer's channel/interceptor construction throwing must not leak the ones already
        // created for earlier peers — the constructor never finishes, so nothing else will ever
        // get a reference to call Dispose() on them.
        try {
            foreach (var peer in peers) {
                var handler = new SocketsHttpHandler {
                    EnableMultipleHttp2Connections = true,
                    KeepAlivePingDelay = TimeSpan.FromSeconds(15),
                    KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
                    KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
                };

                if (credentials is not null) {
                    var expectedSubject = expectedCertificateSubjects?.GetValueOrDefault(peer.NodeId);
                    var validator = new PeerCertificateValidator(credentials.CaCertificate, expectedSubject);
                    handler.SslOptions = new SslClientAuthenticationOptions {
                        ClientCertificates = [credentials.NodeCertificate],
                        RemoteCertificateValidationCallback = (_, certificate, chain, errors) =>
                            certificate is X509Certificate2 candidate && chain is not null && validator.Validate(candidate, chain, errors),
                    };
                }

                var channel = GrpcChannel.ForAddress(peer.Address, new GrpcChannelOptions { HttpHandler = handler, DisposeHttpClient = true });
                _channels[peer.NodeId] = channel;

                CallInvoker invoker = channel.CreateCallInvoker();
                foreach (var interceptor in interceptors)
                    invoker = invoker.Intercept(interceptor);

                _clients[peer.NodeId] = new RaftTransport.RaftTransportClient(invoker);
            }
        } catch {
            Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken cancellationToken = default) {
        using var call = Client(peerId).RequestVoteAsync(request, deadline: DateTime.UtcNow + _rpcTimeout, cancellationToken: cancellationToken);
        return await call.ResponseAsync;
    }

    /// <inheritdoc />
    public async ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        using var call = Client(peerId).AppendEntriesAsync(request, deadline: DateTime.UtcNow + _rpcTimeout, cancellationToken: cancellationToken);
        return await call.ResponseAsync;
    }

    /// <inheritdoc />
    public async ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        using var call = Client(peerId).InstallSnapshotAsync(request, deadline: DateTime.UtcNow + _rpcTimeout, cancellationToken: cancellationToken);
        return await call.ResponseAsync;
    }

    RaftTransport.RaftTransportClient Client(string peerId) => _clients.TryGetValue(peerId, out var client)
        ? client
        : throw new InvalidOperationException($"'{peerId}' is not a configured peer for this transport.");

    /// <summary>
    /// Disposes every underlying channel.
    /// </summary>
    public void Dispose() {
        foreach (var channel in _channels.Values)
            channel.Dispose();
    }
}
