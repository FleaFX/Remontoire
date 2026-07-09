using Remontoire.Raft.V1;
using Remontoire.Storage;

namespace Remontoire.Raft;

/// <summary>
/// A message posted to <see cref="RaftReplica"/>'s actor mailbox. Every mutation of Raft
/// state — role transitions, term changes, log appends, commit advancement — flows through
/// here so it is processed one at a time, against current state, in strict arrival order.
/// Timers, inbound RPCs, RPC responses, and application requests are all just messages;
/// none of them ever touches replica state directly.
/// </summary>
abstract record RaftReplicaMessage;

/// <summary>
/// Fired by the election timer. <paramref name="Generation"/> must equal the replica's current
/// election-timer generation; a stale firing (from a superseded timer) is silently discarded.
/// </summary>
sealed record ElectionTimeoutElapsed(ulong Generation) : RaftReplicaMessage;

/// <summary>
/// Fired by the leader's heartbeat timer. Same generation-staleness rule as
/// <see cref="ElectionTimeoutElapsed"/>.
/// </summary>
sealed record HeartbeatIntervalElapsed(ulong Generation) : RaftReplicaMessage;

/// <summary>
/// Posted when a peer sends a <see cref="VoteRequest"/>. The actor resolves
/// <paramref name="Reply"/> once processed — after durably persisting any term/vote change.
/// </summary>
sealed record VoteRequestReceived(VoteRequest Request, TaskCompletionSource<VoteResponse> Reply) : RaftReplicaMessage;

/// <summary>
/// Posted when the leader sends an <see cref="AppendEntriesRequest"/> (or heartbeat).
/// </summary>
sealed record AppendEntriesReceived(AppendEntriesRequest Request, TaskCompletionSource<AppendEntriesResponse> Reply) : RaftReplicaMessage;

/// <summary>
/// Posted when the leader sends an <see cref="InstallSnapshotRequest"/> chunk.
/// </summary>
sealed record InstallSnapshotReceived(InstallSnapshotRequest Request, TaskCompletionSource<InstallSnapshotResponse> Reply) : RaftReplicaMessage;

/// <summary>
/// Posted by the background task that sent a <see cref="VoteRequest"/> to a peer.
/// <paramref name="SentTerm"/> is the term at which the RPC was fired; the handler discards
/// the response when the current term has since advanced or the role changed.
/// </summary>
sealed record VoteResponseReceived(string PeerId, VoteResponse Response, ulong SentTerm) : RaftReplicaMessage;

/// <summary>
/// Posted by the background task that sent an <see cref="AppendEntriesRequest"/> to a peer.
/// <paramref name="SentUpToIndex"/> is the last index included in that request, so the handler
/// can advance <c>matchIndex</c> without re-deriving what was sent.
/// </summary>
sealed record AppendEntriesResponseReceived(string PeerId, AppendEntriesResponse Response, ulong SentTerm, ulong SentUpToIndex) : RaftReplicaMessage;

/// <summary>
/// Posted by the background task that sent an <see cref="InstallSnapshotRequest"/> chunk.
/// </summary>
sealed record InstallSnapshotResponseReceived(string PeerId, InstallSnapshotResponse Response, ulong SentTerm, ulong SentSnapshotIndex) : RaftReplicaMessage;

/// <summary>
/// Posted by <see cref="RaftReplica.ProposeAsync"/> — an application request to replicate one
/// record. <paramref name="Reply"/> resolves once the entry is quorum-committed, never before.
/// </summary>
sealed record ProposeReceived(AppendRequest Request, TaskCompletionSource<ProposeResult> Reply) : RaftReplicaMessage;

/// <summary>
/// Posted by the background task that ran <c>prepareSnapshot</c> successfully — the log tail up
/// to <paramref name="LastIncludedIndex"/> is now durable in a segment, so it's safe to compact.
/// </summary>
sealed record SnapshotPrepared(ulong LastIncludedIndex, ulong LastIncludedTerm, ulong NextLogicalOffset) : RaftReplicaMessage;

/// <summary>
/// Posted by the background task that ran <c>prepareSnapshot</c> when it threw — clears the
/// in-progress marker so a later commit advance gets to try again.
/// </summary>
sealed record SnapshotPreparationFailed : RaftReplicaMessage;

/// <summary>
/// Used exclusively by tests to detect that the actor has processed every previously injected
/// message: the actor resolves <paramref name="Completion"/> the moment it dequeues this.
/// </summary>
sealed record DrainSentinel(TaskCompletionSource Completion) : RaftReplicaMessage;
