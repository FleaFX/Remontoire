using System.Collections.Concurrent;

namespace Remontoire.Sharding;

/// <summary>
/// The materialized, in-memory shard-assignment state — one instance per process (every
/// server node and every client connection each keep their own), rebuilt entirely by
/// replaying a committed log, never itself separately persisted. Reads are lock-free
/// dictionary lookups — no leader redirect, ever.
/// </summary>
public sealed class ShardAssignmentTable {
    readonly ConcurrentDictionary<string, StreamShardingConfig> _streams = new();
    readonly ConcurrentDictionary<string, PhysicalGroupDescriptor> _groups = new();
    readonly ConcurrentDictionary<(string StreamName, int VirtualShardIndex), VirtualShardAssignment> _assignments = new();

    /// <summary>
    /// Looks up a stream's fixed sharding config, if the stream is known.
    /// </summary>
    public bool TryGetStreamConfig(string streamName, out StreamShardingConfig config) => _streams.TryGetValue(streamName, out config);

    /// <summary>
    /// Looks up a physical group's current membership, if the group is known.
    /// </summary>
    public bool TryGetGroup(string groupId, out PhysicalGroupDescriptor group) => _groups.TryGetValue(groupId, out group);

    /// <summary>
    /// Looks up which physical group currently serves a stream's virtual shard.
    /// </summary>
    public bool TryGetAssignment(string streamName, int virtualShardIndex, out VirtualShardAssignment assignment) =>
        _assignments.TryGetValue((streamName, virtualShardIndex), out assignment);
}
