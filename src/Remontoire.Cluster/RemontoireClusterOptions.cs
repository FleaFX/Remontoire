namespace Remontoire.Cluster;

/// <summary>
/// Configures a cluster node: the data groups it hosts, its meta-group membership (or the meta-group
/// seeds it watches instead), and the handler of its outbound channels. Set in code through
/// <see cref="RemontoireClusterBuilderExtensions.AddCluster"/>; binding it from configuration is the
/// host's choice.
/// </summary>
public sealed class RemontoireClusterOptions {
    /// <summary>
    /// Every physical shard group this node hosts. May be empty on a meta-group-only node.
    /// </summary>
    public List<RaftGroupOptions> Groups { get; set; } = [];

    /// <summary>
    /// This node's meta-group membership, if any — <see langword="null"/> on a node that
    /// doesn't host it.
    /// </summary>
    public MetaGroupOptions? MetaGroup { get; set; }

    /// <summary>
    /// Client address of at least one meta-group member, used to bootstrap a watcher that keeps this
    /// node's local assignment table fresh over the network. Only consulted when
    /// <see cref="MetaGroup"/> is <see langword="null"/> — a node that hosts a meta-group member
    /// reads its own local replica directly instead, no network hop needed.
    /// </summary>
    public List<string> MetaGroupSeedAddresses { get; set; } = [];

    /// <summary>
    /// The address of this node's client endpoint (<c>MapRemontoire</c>), as clients reach it.
    /// Required on a node that hosts a meta-group replica and data groups: it registers those groups
    /// in the meta log, itself included at this address, so clients and new streams find them.
    /// </summary>
    public string? ClientAddress { get; set; }

    /// <summary>
    /// Optional hook to configure the <see cref="SocketsHttpHandler"/> of every channel this node
    /// opens — the Raft channels to its peers (after the transport's keep-alive settings) and the
    /// channel to the meta-group seed — called once per channel on a fresh handler. For example
    /// <see cref="SocketsHttpHandler.SslOptions"/> to present a node certificate and trust a private
    /// CA. <see langword="null"/> (the default) leaves TLS to each address's scheme and the
    /// platform's trust store.
    /// </summary>
    public Action<SocketsHttpHandler>? ConfigureHttpHandler { get; set; }
}

/// <summary>
/// One physical data group this node hosts.
/// </summary>
public sealed class RaftGroupOptions {
    /// <summary>
    /// The physical shard group this node hosts.
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
/// This node's meta-group membership: the shared, cluster-wide assignment-table group, holding
/// zero virtual shards of its own. A separate type from <see cref="RaftGroupOptions"/> rather than
/// a reused one, since the meta-group's fixed-small-voter invariant has no analog on an ordinary
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
/// One other member of a group, as configured for this node.
/// </summary>
public sealed class RaftPeerOptions {
    /// <summary>
    /// The peer's unique identifier within the group.
    /// </summary>
    public string NodeId { get; set; } = "";

    /// <summary>
    /// The address of the peer's Raft endpoint (<c>MapRemontoireCluster</c>).
    /// </summary>
    public string Address { get; set; } = "";

    /// <summary>
    /// The address of the peer's client endpoint (<c>MapRemontoire</c>), when it differs from
    /// <see cref="Address"/>: a redirect sends clients there while this peer leads a group.
    /// <see langword="null"/> (the default) means clients and peers share <see cref="Address"/>.
    /// </summary>
    public string? ClientAddress { get; set; }
}
