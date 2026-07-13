namespace Remontoire.Client;

/// <summary>
/// Configuration for a <see cref="RemontoireConnection"/>.
/// </summary>
/// <param name="MetaGroupSeedAddresses">
/// Address of at least one meta-group member — used only to bootstrap this connection's local
/// assignment table; everything else (which stream lives on which virtual shard, which physical
/// group currently serves it, that group's own member addresses) is learned dynamically from
/// there onward, never configured directly.
/// </param>
/// <param name="MaxRedirectAttempts">How many times a call retries after a NotLeader redirect before giving up.</param>
/// <param name="RedirectRetryDelay">How long to wait before retrying when a redirect carries no leader hint (an election is in progress).</param>
/// <param name="AllowInsecureTransport">
/// Explicitly allows this connection to talk to the cluster unencrypted. Judged solely by this
/// flag, never by an environment-name heuristic. Never use outside development/test.
/// </param>
/// <param name="ClusterCaCertificatePath">
/// Path to the cluster's own CA certificate — needed to trust the cluster's TLS certificate when
/// it isn't already signed by an OS-trusted root (e.g. the same cluster-internal CA node-to-node
/// mTLS uses). <see langword="null"/> falls back to the platform's default trust store, correct
/// only when the cluster's client-facing certificate is issued by a real, publicly trusted CA.
/// </param>
public sealed record RemontoireClientOptions(
    IReadOnlyList<Uri> MetaGroupSeedAddresses,
    int MaxRedirectAttempts = 5,
    TimeSpan RedirectRetryDelay = default,
    bool AllowInsecureTransport = false,
    string? ClusterCaCertificatePath = null
);
