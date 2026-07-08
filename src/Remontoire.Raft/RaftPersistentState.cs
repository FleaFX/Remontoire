namespace Remontoire.Raft;

/// <summary>
/// The two values Raft requires to be durable before any RPC response that depends on them
/// is sent: the highest term seen, and who received this node's vote in that term.
/// </summary>
public readonly record struct RaftPersistentState(ulong CurrentTerm, string? VotedFor);
