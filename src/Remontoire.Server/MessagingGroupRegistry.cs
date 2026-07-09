using System.Collections.Concurrent;
using Remontoire.Messaging;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// Per group: the materialized <see cref="ShardLog"/> a <see cref="Raft.RaftReplica"/> feeds, and
/// the <see cref="AckIndex"/> maintained from its republished stream. Same "registry, not a
/// singleton for one group" shape as <see cref="Raft.Grpc.RaftReplicaRegistry"/> — a later phase
/// registers more than one.
/// </summary>
public sealed class MessagingGroupRegistry {
    readonly ConcurrentDictionary<string, (ShardLog ShardLog, AckIndex AckIndex)> _groups = new();

    /// <summary>
    /// Registers (or overwrites) <paramref name="groupId"/>'s shard log and ack index.
    /// </summary>
    public void Register(string groupId, ShardLog shardLog, AckIndex ackIndex) => _groups[groupId] = (shardLog, ackIndex);

    /// <summary>
    /// Removes whatever is registered for <paramref name="groupId"/>, if anything.
    /// </summary>
    public void Unregister(string groupId) => _groups.TryRemove(groupId, out _);

    /// <summary>
    /// Looks up the shard log and ack index owning <paramref name="groupId"/>, if any is currently registered.
    /// </summary>
    public bool TryGet(string groupId, out (ShardLog ShardLog, AckIndex AckIndex) value) => _groups.TryGetValue(groupId, out value);
}
