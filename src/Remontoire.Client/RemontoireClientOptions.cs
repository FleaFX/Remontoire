namespace Remontoire.Client;

/// <summary>
/// Configuration for a <see cref="RemontoireConnection"/>.
/// </summary>
/// <param name="StreamGroupIds">
/// Static streamName → groupId mapping — a temporary simplification until a control plane exists
/// to store this; a stream is served by exactly one physical group for now.
/// </param>
/// <param name="GroupMemberAddresses">Every known member address of the group(s) this connection talks to.</param>
/// <param name="MaxRedirectAttempts">How many times a call retries after a NotLeader redirect before giving up.</param>
/// <param name="RedirectRetryDelay">How long to wait before retrying when a redirect carries no leader hint (an election is in progress).</param>
public sealed record RemontoireClientOptions(
    IReadOnlyDictionary<string, string> StreamGroupIds,
    IReadOnlyList<Uri> GroupMemberAddresses,
    int MaxRedirectAttempts = 5,
    TimeSpan RedirectRetryDelay = default
);
