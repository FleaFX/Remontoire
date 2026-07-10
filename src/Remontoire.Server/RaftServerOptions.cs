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

    /// <summary>
    /// This node's meta-group membership, if any — <see langword="null"/> on a node that
    /// doesn't host it.
    /// </summary>
    public MetaGroupOptions? MetaGroup { get; set; }
}

/// <summary>
/// This node's meta-group membership: the shared, cluster-wide assignment-table group, holding
/// zero virtual shards of its own. A separate type from the data-group shape above rather than a
/// reused one, since the meta-group's fixed-small-voter invariant has no analog on an ordinary
/// data group.
/// </summary>
public sealed class MetaGroupOptions {
    /// <summary>
    /// This node's unique identifier within the meta-group.
    /// </summary>
    public string NodeId { get; set; } = "";

    /// <summary>
    /// Directory holding the meta-group's WAL and small durable Raft state.
    /// </summary>
    public string DataDirectory { get; set; } = "";

    /// <summary>
    /// The meta-group's voting members — fixed and small, not meant to grow with cluster size.
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
