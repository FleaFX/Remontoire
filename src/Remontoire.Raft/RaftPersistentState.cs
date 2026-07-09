namespace Remontoire.Raft;

/// <summary>
/// The small set of facts Raft requires to be durable before something that depends on them is
/// allowed to proceed: the highest term seen and who received this node's vote in that term
/// (before any RPC response depending on them is sent), and the logical offset the log's own
/// compacted base was taken at (before the corresponding <see cref="IRaftLog.CompactToAsync"/>
/// call — see <c>RaftReplica.Leader.cs</c>'s <c>RecoverNextLogicalOffsetAsync</c>, which floors
/// its scan on this value).
/// </summary>
public readonly record struct RaftPersistentState(ulong CurrentTerm, string? VotedFor, ulong SnapshotNextLogicalOffset = 0);
