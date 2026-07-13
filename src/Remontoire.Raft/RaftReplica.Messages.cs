using System.Diagnostics;
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
/// Posted once by the background task that ran a full <c>InstallSnapshot</c> transfer to a peer,
/// after its last chunk's response — not once per chunk. <paramref name="SentSnapshotIndex"/> is
/// the transfer's own <c>lastIncludedIndex</c>, so the handler can advance <c>nextIndex</c>/
/// <c>matchIndex</c> straight to it without re-deriving what was sent.
/// </summary>
sealed record InstallSnapshotResponseReceived(string PeerId, InstallSnapshotResponse Response, ulong SentTerm, ulong SentSnapshotIndex) : RaftReplicaMessage;

/// <summary>
/// Posted by the background task that ran an <c>InstallSnapshot</c> transfer when it threw
/// (network failure, cancelled, or the disk read of a segment failed) — clears the in-flight
/// marker for <paramref name="PeerId"/> so the next heartbeat tick retries from scratch.
/// </summary>
sealed record InstallSnapshotTransferFailed(string PeerId) : RaftReplicaMessage;

/// <summary>
/// Posted by <see cref="RaftReplica.ProposeAsync(AppendRequest, CancellationToken)"/> — an
/// application request to replicate one record. <paramref name="Reply"/> resolves once the entry
/// is quorum-committed, never before. <paramref name="CallerContext"/> is <c>Activity.Current</c>'s
/// context at the moment this was posted, captured explicitly because the actor loop runs on its
/// own detached <c>Task.Run</c> — <c>AsyncLocal</c>-based ambient context does not flow across that
/// boundary on its own, so the caller's tracing context has to be carried on the message itself.
/// </summary>
sealed record ProposeReceived(AppendRequest Request, TaskCompletionSource<ProposeResult> Reply, ActivityContext? CallerContext = null) : RaftReplicaMessage;

/// <summary>
/// Posted by <see cref="RaftReplica.ProposeAsync(AckRequest, CancellationToken)"/> —
/// same commit-then-resolve contract as <see cref="ProposeReceived"/>, minus the
/// <see cref="ProposeResult.LogicalOffset"/> assignment. Same <see cref="CallerContext"/> capture
/// reasoning as <see cref="ProposeReceived"/>.
/// </summary>
sealed record ProposeAckReceived(AckRequest Request, TaskCompletionSource<ProposeResult> Reply, ActivityContext? CallerContext = null) : RaftReplicaMessage;

/// <summary>
/// Posted by <see cref="RaftReplica.ProposeAsync(AckCheckpointRequest, CancellationToken)"/> —
/// same commit-then-resolve contract as <see cref="ProposeReceived"/>, minus the
/// <see cref="ProposeResult.LogicalOffset"/> assignment. Same <see cref="CallerContext"/> capture
/// reasoning as <see cref="ProposeReceived"/>.
/// </summary>
sealed record ProposeAckCheckpointReceived(AckCheckpointRequest Request, TaskCompletionSource<ProposeResult> Reply, ActivityContext? CallerContext = null) : RaftReplicaMessage;

/// <summary>
/// Posted by <see cref="RaftReplica.ProposeConfigChangeAsync"/>. <paramref name="Reply"/>
/// resolves once the change is quorum-committed — it takes effect on this replica immediately on
/// append, well before that, per the paper.
/// </summary>
sealed record ProposeConfigChangeReceived(IReadOnlyList<RaftGroupMember> FullMembership, TaskCompletionSource Reply) : RaftReplicaMessage;

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
