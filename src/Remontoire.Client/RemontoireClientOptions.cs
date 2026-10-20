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
/// <param name="ConfigureHttpHandler">
/// Optional hook to configure the <see cref="SocketsHttpHandler"/> of every channel this connection
/// opens, called once per channel on a fresh handler — for example <see cref="SocketsHttpHandler.SslOptions"/>
/// to trust a private CA or to present a client certificate. <see langword="null"/> (the default)
/// uses the default handler, with TLS decided by each address's scheme and the platform's trust store.
/// </param>
/// <param name="GetAccessTokenAsync">
/// Optional hook attaching a bearer token to every outbound call, via <see cref="Grpc.Core.CallCredentials.FromInterceptor"/>.
/// <see langword="null"/> (the default) sends no token at all. Acquiring/refreshing the token
/// itself is entirely this delegate's own concern; this connection only ever calls it once per
/// outbound call.
/// </param>
public sealed record RemontoireClientOptions(
    IReadOnlyList<Uri> MetaGroupSeedAddresses,
    int MaxRedirectAttempts = 5,
    TimeSpan RedirectRetryDelay = default,
    Action<SocketsHttpHandler>? ConfigureHttpHandler = null,
    Func<CancellationToken, Task<string>>? GetAccessTokenAsync = null
);
