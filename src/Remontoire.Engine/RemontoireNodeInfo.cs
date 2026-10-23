using Remontoire.Sharding;

namespace Remontoire.Engine;

/// <summary>
/// What the hosting packages need to know about this node, embedded or a cluster node.
/// </summary>
/// <param name="HostsMetaGroupReplica">Whether this node hosts a meta-group replica, and so can serve the shard-assignment feed and declare.</param>
/// <param name="OwnGroupMember">
/// Embedded: the only member of the group every stream gets for itself. <see langword="null"/> on a
/// cluster node, where a new stream goes to a registered data group.
/// </param>
public sealed record RemontoireNodeInfo(bool HostsMetaGroupReplica, ShardGroupMember? OwnGroupMember = null);
