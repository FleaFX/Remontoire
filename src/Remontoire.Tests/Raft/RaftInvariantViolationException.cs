namespace Remontoire.Raft;

/// <summary>
/// Thrown by <see cref="RaftInvariantChecker"/> when a simulated run observes a violation of one
/// of Raft's five safety properties. Reaching this is always a bug, never an expected outcome —
/// unlike <see cref="NotLeaderException"/>, there is no caller that should ever catch this.
/// </summary>
sealed class RaftInvariantViolationException(string message) : Exception(message);
