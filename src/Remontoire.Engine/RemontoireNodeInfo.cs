namespace Remontoire.Engine;

/// <summary>
/// What the hosting packages need to know about this node, embedded or a cluster node.
/// </summary>
/// <param name="HostsMetaGroupReplica">Whether this node hosts a meta-group replica, and so can serve the shard-assignment feed.</param>
public sealed record RemontoireNodeInfo(bool HostsMetaGroupReplica);
