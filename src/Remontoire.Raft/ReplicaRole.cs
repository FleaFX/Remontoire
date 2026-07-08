namespace Remontoire.Raft;

/// <summary>
/// Distinguishes the three Raft roles a <see cref="RaftReplica"/> occupies at any moment.
/// </summary>
enum ReplicaRole : byte {
    /// <summary>Passively replicates entries from the leader. Initial role at startup.</summary>
    Follower = 0,

    /// <summary>Soliciting votes in an attempt to win a leader election.</summary>
    Candidate = 1,

    /// <summary>Accepts proposals and drives replication.</summary>
    Leader = 2,
}
