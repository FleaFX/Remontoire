using Remontoire.Security;

namespace Remontoire.Server;

/// <summary>
/// Binds the "Raft" configuration section: every physical shard group this process hosts, plus
/// (optionally) this node's meta-group membership. A process may now host zero or more data
/// groups and, separately, zero or one meta-group replica.
/// </summary>
public sealed class RaftServerOptions {
    /// <summary>
    /// Every physical shard group this process hosts. May be empty on a meta-group-only node.
    /// </summary>
    public List<RaftGroupOptions> Groups { get; set; } = [];

    /// <summary>
    /// This node's meta-group membership, if any — <see langword="null"/> on a node that
    /// doesn't host it.
    /// </summary>
    public MetaGroupOptions? MetaGroup { get; set; }

    /// <summary>
    /// Address of at least one meta-group member, used to bootstrap a watcher that keeps this
    /// node's local assignment table fresh over the network. Only consulted when
    /// <see cref="MetaGroup"/> is <see langword="null"/> — a node that hosts a meta-group member
    /// reads its own local replica directly instead, no network hop needed.
    /// </summary>
    public List<string> MetaGroupSeedAddresses { get; set; } = [];

    /// <summary>
    /// Below this many free bytes on any hosted group's (or the meta-group's) data directory,
    /// readiness fails (<c>DiskSpaceReadinessCheck</c>). Starting point: 1 GiB — a reasonable
    /// default, not a value tuned against any real production disk-pressure data yet.
    /// </summary>
    public long MinFreeDiskSpaceBytes { get; set; } = 1L * 1024 * 1024 * 1024;

    /// <summary>
    /// Node-to-node mTLS configuration, or an explicit opt-out into insecure transport (dev/test only).
    /// </summary>
    public ClusterMtlsOptions Mtls { get; set; } = new();

    /// <summary>
    /// The port <see cref="Remontoire.Raft.Grpc.RaftTransportGrpcService"/> listens on — mTLS-required.
    /// </summary>
    public int PeerPort { get; set; }

    /// <summary>
    /// The port the client-facing gRPC services listen on — ordinary TLS, no client certificate.
    /// </summary>
    public int ClientPort { get; set; }
}

/// <summary>
/// One physical data group this process hosts — same shape as the whole <see cref="RaftServerOptions"/>
/// used to be before a process could host more than one.
/// </summary>
public sealed class RaftGroupOptions {
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
/// One other member of a group, as configured for this process.
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

    /// <summary>
    /// The subject this peer's certificate must carry — a certificate merely signed by the
    /// cluster CA is not enough: a compromised-but-CA-issued certificate for the wrong node would
    /// otherwise go undetected. <see langword="null"/> means no additional subject check beyond
    /// the CA chain itself.
    /// </summary>
    public string? ExpectedCertificateSubject { get; set; }
}
