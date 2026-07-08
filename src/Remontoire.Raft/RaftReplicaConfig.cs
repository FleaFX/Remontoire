namespace Remontoire.Raft;

/// <summary>
/// Immutable runtime configuration for one <see cref="RaftReplica"/>. This same object is what
/// the transport is constructed from — deliberately, so the RPC timeout can never silently
/// fail to reach the transport (there is no second place to forget it).
/// </summary>
/// <param name="GroupId">The physical shard group this replica belongs to.</param>
/// <param name="NodeId">This node's unique identifier within the group.</param>
/// <param name="Peers">All other members of the group; empty for a single-node (test) group.</param>
/// <param name="HeartbeatInterval">Leader heartbeat cadence. Starting point: 50 ms.</param>
/// <param name="ElectionTimeoutMin">Lower bound of the randomized election timeout. Starting point: 250 ms.</param>
/// <param name="ElectionTimeoutMax">Upper bound of the randomized election timeout. Starting point: 500 ms.</param>
/// <param name="RpcTimeout">
/// Deadline applied to every outbound Raft RPC. Defaults to five times
/// <param name="HeartbeatInterval"/> when not set explicitly.</param>
/// <param name="SnapshotChunkSizeBytes">Maximum bytes per <c>InstallSnapshot</c> chunk. Default 1 MiB.</param>
/// <param name="ElectionRandomSeed">Optional seed for the election-timeout randomization</param>
public sealed record RaftReplicaConfig(
    string GroupId,
    string NodeId,
    IReadOnlyList<RaftGroupMember> Peers,
    TimeSpan HeartbeatInterval,
    TimeSpan ElectionTimeoutMin,
    TimeSpan ElectionTimeoutMax,
    TimeSpan? RpcTimeout = null,
    int SnapshotChunkSizeBytes = 1 << 20,
    int? ElectionRandomSeed = null
);
