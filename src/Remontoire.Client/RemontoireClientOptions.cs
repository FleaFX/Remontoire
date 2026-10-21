namespace Remontoire.Client;

/// <summary>
/// Configuration for a <see cref="RemontoireConnection"/>.
/// </summary>
public sealed class RemontoireClientOptions {
    /// <summary>
    /// Client address of at least one meta-group member — used only to bootstrap the connection's
    /// local assignment table; everything else (which stream lives on which virtual shard, which
    /// physical group currently serves it, that group's own member addresses) is learned
    /// dynamically from there onward, never configured directly.
    /// </summary>
    public IReadOnlyList<Uri> MetaGroupSeedAddresses { get; set; } = [];

    /// <summary>
    /// How many times a call retries after a NotLeader redirect before giving up. Defaults to 5.
    /// </summary>
    public int MaxRedirectAttempts { get; set; } = 5;

    /// <summary>
    /// How long to wait before retrying when a redirect carries no leader hint (an election is in
    /// progress). Defaults to zero.
    /// </summary>
    public TimeSpan RedirectRetryDelay { get; set; }

    /// <summary>
    /// Optional hook to configure the <see cref="SocketsHttpHandler"/> of every channel the
    /// connection opens, called once per channel on a fresh handler — for example
    /// <see cref="SocketsHttpHandler.SslOptions"/> to trust a private CA or to present a client
    /// certificate. <see langword="null"/> (the default) uses the default handler, with TLS decided
    /// by each address's scheme and the platform's trust store.
    /// </summary>
    public Action<SocketsHttpHandler>? ConfigureHttpHandler { get; set; }

    /// <summary>
    /// Optional hook attaching a bearer token to every outbound call, via
    /// <see cref="Grpc.Core.CallCredentials.FromInterceptor"/>. <see langword="null"/> (the default)
    /// sends no token at all. Acquiring/refreshing the token itself is entirely this delegate's own
    /// concern; the connection only ever calls it once per outbound call.
    /// </summary>
    public Func<CancellationToken, Task<string>>? GetAccessTokenAsync { get; set; }
}
