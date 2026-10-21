namespace Remontoire.Sharding;

/// <summary>
/// Identifies one physical Raft group and how to reach it — the unit <see cref="VirtualShardAssignment"/>
/// points a virtual shard at. Deliberately carries member addresses inline rather than a bare
/// group id: a client resolving a virtual shard needs somewhere to actually connect.
/// </summary>
/// <param name="GroupId">Matches the physical group's own identifier exactly.</param>
/// <param name="Members">Every current member of the group: node id + gRPC address.</param>
public readonly record struct PhysicalGroupDescriptor(string GroupId, IReadOnlyList<ShardGroupMember> Members);

/// <summary>
/// Node id + gRPC address. An equivalent shape may already exist elsewhere in the solution,
/// but it's deliberately redefined here rather than reused, so that this project never gains
/// a dependency it doesn't otherwise need.
/// </summary>
public readonly record struct ShardGroupMember(string NodeId, Uri Address);
