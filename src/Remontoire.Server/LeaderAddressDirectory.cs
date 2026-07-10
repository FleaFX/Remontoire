using System.Collections.Concurrent;

namespace Remontoire.Server;

/// <summary>
/// Translates a Raft node ID to its gRPC address — <see cref="Raft.RaftReplica.LeaderHint"/> is a
/// bare node ID (the right concept inside a Raft group itself), but a client needs an address to
/// connect to. Shared across every group this process hosts, populated from each group's own
/// configured peers — the same source <see cref="Raft.Grpc.RaftGrpcTransport"/> already uses to
/// build its own peer channels, never a second source of truth.
/// </summary>
public sealed class LeaderAddressDirectory {
    readonly ConcurrentDictionary<string, Uri> _addresses = new();

    /// <summary>
    /// Registers (or overwrites) <paramref name="nodeId"/>'s address.
    /// </summary>
    public void Register(string nodeId, Uri address) => _addresses[nodeId] = address;

    /// <summary>
    /// The address registered for <paramref name="nodeId"/>, or <see langword="null"/> if none is known.
    /// </summary>
    public Uri? TryGet(string nodeId) => _addresses.TryGetValue(nodeId, out var address) ? address : null;
}
