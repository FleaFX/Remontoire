namespace Remontoire.Server;

/// <summary>
/// Binds the "Raft" configuration section: the single physical shard group this process hosts.
/// </summary>
public sealed class RaftServerOptions {
    /// <summary>
    /// The physical shard group this process hosts.
    /// </summary>
    public string GroupId { get; set; } = "";

    /// <summary>
    /// This node's unique identifier within the group.
    /// </summary>
    public string NodeId { get; set; } = "";

    /// <summary>
    /// Directory holding this group's WAL and small durable Raft state.
    /// </summary>
    public string DataDirectory { get; set; } = "";

    /// <summary>
    /// All other members of the group.
    /// </summary>
    public List<RaftPeerOptions> Peers { get; set; } = [];
}

/// <summary>
/// One other member of the group, as configured for this process.
/// </summary>
public sealed class RaftPeerOptions {
    /// <summary>
    /// The peer's unique identifier within the group.
    /// </summary>
    public string NodeId { get; set; } = "";

    /// <summary>
    /// The peer's gRPC address.
    /// </summary>
    public string Address { get; set; } = "";
}
