using System.Threading.Channels;

namespace Remontoire.Storage.Compaction;

/// <summary>
/// Runs one merge at a time, off the actor's own thread. Asks the actor what to compact via
/// <see cref="CompactionPlanRequest"/>, does the merge I/O, then reports back via
/// <see cref="CompactionCompleted"/>. Needs nothing from the actor beyond a way to post
/// messages — it never touches the actor's MemTable/segments state directly.
/// </summary>
sealed class CompactionWorker(ChannelWriter<ShardLogMessage> mailbox, Action<TimeSpan>? onDurationMeasured = null) {
    /// <summary>
    /// Loops forever, always keeping exactly one plan request outstanding, until the actor's
    /// mailbox closes (normal shutdown) or an outstanding request gets cancelled (shutdown
    /// while nothing was left to compact).
    /// </summary>
    public async Task RunAsync() {
        while (true) {
            var response = new TaskCompletionSource<CompactionPlan>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!mailbox.TryWrite(new CompactionPlanRequest(response)))
                return; // mailbox closed — the actor is shutting down

            CompactionPlan plan;
            try {
                plan = await response.Task;
            } catch (OperationCanceledException) {
                return; // the actor shut down while this request was still outstanding
            }

            string? mergedPath = null;
            Exception? error = null;
            try {
                mergedPath = await plan.MergeAsync(onDurationMeasured);
            } catch (Exception ex) {
                error = ex;
            }

            if (!mailbox.TryWrite(new CompactionCompleted(plan, mergedPath, error)))
                return; // mailbox closed in the meantime — the result is simply discarded, harmless
        }
    }
}
