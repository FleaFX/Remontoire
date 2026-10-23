namespace Remontoire;

/// <summary>
/// Thrown when Remontoire cannot be reached in time: no leader for a group after every attempt
/// (no quorum, or a network partition), or a state that a retry may resolve. Never an unbounded wait.
/// </summary>
public sealed class RemontoireUnavailableException : Exception {
    /// <summary>
    /// Creates the exception for a call that exhausted its attempts to reach a leader.
    /// </summary>
    /// <param name="groupId">The group the call was trying to reach.</param>
    /// <param name="attempts">How many attempts were made.</param>
    /// <param name="lastFailure">The failure of the last attempt, if any.</param>
    public RemontoireUnavailableException(string groupId, int attempts, Exception? lastFailure = null)
        : base($"Could not reach a leader for group '{groupId}' after {attempts} attempt(s).", lastFailure) {
        GroupId = groupId;
        Attempts = attempts;
    }

    /// <summary>
    /// Creates the exception for a state of <paramref name="groupId"/> that a retry may resolve.
    /// </summary>
    /// <param name="groupId">The group concerned.</param>
    /// <param name="message">What is not available yet.</param>
    public RemontoireUnavailableException(string groupId, string message) : base(message) => GroupId = groupId;

    /// <summary>
    /// The group this call was trying to reach.
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// How many attempts were made before giving up; zero when the exception names a state instead.
    /// </summary>
    public int Attempts { get; }
}
