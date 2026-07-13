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
public sealed record RemontoireClientOptions(
    IReadOnlyList<Uri> MetaGroupSeedAddresses,
    int MaxRedirectAttempts = 5,
    TimeSpan RedirectRetryDelay = default,
    bool AllowInsecureTransport = false
);
