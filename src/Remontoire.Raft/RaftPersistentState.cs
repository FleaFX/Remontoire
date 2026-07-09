namespace Remontoire.Raft;

/// <summary>
/// The small set of facts Raft requires to be durable before something that depends on them is
/// allowed to proceed: the highest term seen and who received this node's vote in that term
/// (before any RPC response depending on them is sent); the logical offset the log's own
/// compacted base was taken at (before the corresponding <see cref="IRaftLog.CompactToAsync"/>
/// call — see <c>RaftReplica.Leader.cs</c>'s <c>RecoverNextLogicalOffsetAsync</c>, which floors
/// its scan on this value); and the group's membership as of that same compacted base (a
/// serialized <c>ShardConfiguration</c>, opaque here for the same reason <c>WalRecord.Payload</c>
/// is — see <c>RaftReplica.Membership.cs</c>'s recovery scan, which floors on this the same way).
/// </summary>
public readonly record struct RaftPersistentState(
    ulong CurrentTerm, string? VotedFor, ulong SnapshotNextLogicalOffset = 0, byte[]? SnapshotConfiguration = null);
