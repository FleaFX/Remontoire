namespace Remontoire.Client;

/// <summary>
/// Thrown when a call exhausts every redirect attempt without reaching a leader — a real, sustained
/// outage (no quorum, or a network partition cutting this client off from the whole group), never
/// an unbounded wait.
/// </summary>
public sealed class RemontoireUnavailableException(string groupId, int attempts)
    : Exception($"Could not reach a leader for group '{groupId}' after {attempts} attempt(s).") {
    /// <summary>
    /// The group this call was trying to reach.
    /// </summary>
    public string GroupId { get; } = groupId;

    /// <summary>
    /// How many redirect attempts were made before giving up.
    /// </summary>
    public int Attempts { get; } = attempts;
}
