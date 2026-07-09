namespace Remontoire.Raft;

/// <summary>
/// Thrown when an operation requires the ready group leader (<see cref="RaftReplica.IsLeader"/>)
/// and this replica is not it. Carries the best-known redirect target.
/// </summary>
public sealed class NotLeaderException(string groupId, string? leaderHint)
    : InvalidOperationException($"This replica is not the ready leader of group '{groupId}'.") {
    /// <summary>
    /// The physical shard group the rejected operation targeted.
    /// </summary>
    public string GroupId { get; } = groupId;

    /// <summary>
    /// The node this replica believes is the current leader, or <see langword="null"/> when no
    /// leader is known (an election is in progress).
    /// </summary>
    public string? LeaderHint { get; } = leaderHint;
}
