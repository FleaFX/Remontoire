namespace Remontoire.Raft;

/// <summary>
/// Identifies a peer member of a physical shard group.
/// </summary>
public sealed record RaftGroupMember(string NodeId, Uri Address);
