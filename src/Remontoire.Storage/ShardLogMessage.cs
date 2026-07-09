using Remontoire.Storage.Compaction;

namespace Remontoire.Storage;

/// <summary>
/// A message posted to <see cref="ShardLog"/>'s actor mailbox. Every mutation of the actor's
/// own state — append, WAL-apply, compaction — flows through here so it's processed one at a
/// time, in strict arrival order, regardless of which task or worker posted it.
/// </summary>
abstract record ShardLogMessage;

/// <summary>
/// Posted by the tailing loop for every record the injected committed-source yields — the
/// actor's only way of learning a record is committed and ready to apply to the MemTable. Also
/// reachable directly via <see cref="ShardLog.TryPost"/>, for tests that inject records without
/// a real committed-source.
/// </summary>
sealed record WalRecordCommitted(WalRecord Record) : ShardLogMessage;

/// <summary>
/// Posted by <see cref="Compaction.CompactionWorker"/> once a merge finishes. <paramref name="MergedPath"/>
/// is <see langword="null"/> when the merge itself failed (<paramref name="Error"/> set) —
/// the actor then leaves its segment list untouched.
/// </summary>
sealed record CompactionCompleted(CompactionPlan Plan, string? MergedPath, Exception? Error) : ShardLogMessage;

/// <summary>
/// Posted by <see cref="Compaction.CompactionWorker"/> to ask the actor what to compact next.
/// <paramref name="Response"/> is completed immediately if a plan is ready, or held by the actor
/// (see <c>ShardLog.TryFulfillPendingPlanRequest</c>) until a later flush makes one possible.
/// </summary>
sealed record CompactionPlanRequest(TaskCompletionSource<CompactionPlan> Response) : ShardLogMessage;
