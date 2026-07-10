using Remontoire.Storage;

namespace Remontoire.Raft;

/// <summary>
/// What a successfully committed proposal reports back: the entry's position in the group's
/// Raft log, and — for <see cref="WalRecordType.Append"/> proposals — the consumer-visible
/// logical offset the leader assigned (zero, and meaningless, for any other record type). Also
/// carries the leader's own ingest timestamp — the moment it accepted the proposal, not whatever
/// time a caller elsewhere might otherwise re-derive. Only ever observed after quorum commit.
/// </summary>
public readonly record struct ProposeResult(ulong RaftIndex, ulong LogicalOffset, ulong TimestampMicros);
