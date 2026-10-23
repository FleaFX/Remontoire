using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Remontoire.Engine;
using Remontoire.Raft.V1;
using Remontoire.Storage;

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

    static readonly TimeSpan ForwardTimeout = TimeSpan.FromSeconds(5);

    /// <param name="peers">The reachable peer nodes.</param>
    /// <param name="rpcTimeout">Deadline applied to every outbound RPC, no exceptions.</param>
    /// <param name="configureHandler">
    /// Optional hook to configure each peer channel's <see cref="SocketsHttpHandler"/>, called once
    /// per peer on a fresh handler after the keep-alive settings are applied — for example
    /// <see cref="SocketsHttpHandler.SslOptions"/> to present a node certificate and trust a private
    /// CA. <see langword="null"/> leaves TLS to each peer address's scheme and the platform's trust store.
    /// </param>
    /// <param name="interceptors">Client interceptors applied to every channel — empty by default.</param>
    public RaftGrpcTransport(
        IReadOnlyList<RaftGroupMember> peers, TimeSpan rpcTimeout,
        Action<SocketsHttpHandler>? configureHandler = null, IReadOnlyList<Interceptor>? interceptors = null) {
        _rpcTimeout = rpcTimeout;
        interceptors ??= [];

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
                configureHandler?.Invoke(handler);

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

    /// <summary>
    /// Forwards one meta-log proposal to <paramref name="peerId"/>, the meta-group leader as this node
    /// knows it, and awaits its commit there.
    /// </summary>
    /// <param name="peerId">The node to forward to.</param>
    /// <param name="request">The proposal.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The commit's result on the leader.</returns>
    /// <exception cref="NotLeaderException"><paramref name="peerId"/> is no peer of this node, can't be reached, or doesn't lead the meta group.</exception>
    public async Task<ProposeResult> ProposeMetaAsync(string peerId, AppendRequest request, CancellationToken cancellationToken = default) {
        if (!_clients.TryGetValue(peerId, out var client))
            throw new NotLeaderException(RemontoireNode.MetaGroupId, leaderHint: null);

        var proto = new ProposeMetaRequest { Payload = ByteString.CopyFrom(request.Payload.Span) };
        proto.Headers.Add(request.Headers.Select(header => new ProposeMetaHeader { Key = ByteString.CopyFrom(header.Key.Span), Value = ByteString.CopyFrom(header.Value.Span) }));

        ProposeMetaResponse response;
        try {
            // A commit takes longer than a Raft RPC: its own, longer deadline.
            using var call = client.ProposeMetaAsync(proto, deadline: DateTime.UtcNow + ForwardTimeout, cancellationToken: cancellationToken);
            response = await call.ResponseAsync;
        } catch (RpcException) when (!cancellationToken.IsCancellationRequested) {
            throw new NotLeaderException(RemontoireNode.MetaGroupId, leaderHint: null);
        }

        return response.ResultCase == ProposeMetaResponse.ResultOneofCase.Success
            ? new ProposeResult(response.Success.RaftIndex, response.Success.LogicalOffset, response.Success.TimestampMicros)
            : throw new NotLeaderException(RemontoireNode.MetaGroupId, response.NotLeader.HasLeaderHint ? response.NotLeader.LeaderHint : null);
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
