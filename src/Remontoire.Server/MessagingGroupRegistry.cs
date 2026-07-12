using System.Collections.Concurrent;
using Remontoire.Messaging;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// Per group: the materialized <see cref="ShardLog"/> a <see cref="Raft.RaftReplica"/> feeds, the
/// <see cref="AckIndex"/> maintained from its republished stream, and the <see cref="RetentionEvaluator"/>
/// evaluating its retention/dead-letter policy. Same "registry, not a singleton for one group" shape
/// as <see cref="Raft.Grpc.RaftReplicaRegistry"/> — a later phase registers more than one.
/// </summary>
public sealed class MessagingGroupRegistry {
    readonly ConcurrentDictionary<string, (ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator)> _groups = new();

    /// <summary>
    /// Registers (or overwrites) <paramref name="groupId"/>'s shard log, ack index, and retention evaluator.
    /// </summary>
    public void Register(string groupId, ShardLog shardLog, AckIndex ackIndex, RetentionEvaluator retentionEvaluator) =>
        _groups[groupId] = (shardLog, ackIndex, retentionEvaluator);

    /// <summary>
    /// Removes whatever is registered for <paramref name="groupId"/>, if anything.
    /// </summary>
    public void Unregister(string groupId) => _groups.TryRemove(groupId, out _);

    /// <summary>
    /// Looks up the shard log, ack index, and retention evaluator owning <paramref name="groupId"/>, if any is currently registered.
    /// </summary>
    public bool TryGet(string groupId, out (ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator) value) =>
        _groups.TryGetValue(groupId, out value);

    /// <summary>
    /// Every group currently registered — a live snapshot, not a fixed collection: a group
    /// registered or unregistered after this property is read is not reflected in the snapshot
    /// already handed out.
    /// </summary>
    public IReadOnlyCollection<(string GroupId, ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator)> All =>
        _groups.Select(kvp => (kvp.Key, kvp.Value.ShardLog, kvp.Value.AckIndex, kvp.Value.RetentionEvaluator)).ToArray();
}
