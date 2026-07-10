using System.Collections.Concurrent;

namespace Remontoire.Raft.Grpc;

/// <summary>
/// Resolves an inbound RPC's <c>group_id</c> to the <see cref="RaftReplica"/> that owns it.
/// Phase 3 registers exactly one; the indirection itself is the day-one reservation for phase 5's
/// many groups per node — <see cref="RaftTransportGrpcService"/> never hardcodes "the" replica.
/// </summary>
public sealed class RaftReplicaRegistry {
    readonly ConcurrentDictionary<string, RaftReplica> _replicas = new();

    /// <summary>
    /// Registers <paramref name="replica"/> under its own <see cref="RaftReplica.GroupId"/>.
    /// </summary>
    public void Register(RaftReplica replica) => _replicas[replica.GroupId] = replica;

    /// <summary>
    /// Removes whatever is registered for <paramref name="groupId"/>, if anything.
    /// </summary>
    public void Unregister(string groupId) => _replicas.TryRemove(groupId, out _);

    /// <summary>
    /// Looks up the replica owning <paramref name="groupId"/>, if any is currently registered.
    /// </summary>
    public bool TryGet(string groupId, out RaftReplica replica) => _replicas.TryGetValue(groupId, out replica!);
}
