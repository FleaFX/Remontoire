using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Remontoire.Storage.Compaction;

/// <summary>
/// Periodically enforces a directory's size-based emergency ceiling, off the actor's own thread.
/// Unlike <see cref="CompactionWorker"/>, no request/response round-trip with the actor is needed
/// first: there's nothing to wait for becoming possible (the ceiling check is always immediately
/// answerable), so this simply ticks, runs <see cref="Compactor.PruneOldestUntilUnderSizeAsync"/>
/// directly, and reports back via <see cref="SizePruneCompleted"/>.
/// </summary>
sealed class SizePruneWorker(
    string directory, Func<long?> getMaxTotalBytes, Func<bool>? isAdmissionPaused, ChannelWriter<ShardLogMessage> mailbox,
    TimeProvider? timeProvider = null, TimeSpan? tickInterval = null, ILogger? logger = null) {
    static readonly TimeSpan DefaultTickInterval = TimeSpan.FromMinutes(5);
    long _failedTicksTotal;
    long _messagesPrunedTotal;

    /// <summary>
    /// Running count of ticks that failed (e.g. a corrupt or locked segment) and were silently
    /// retried next tick — otherwise a persistently broken emergency-pruning path (exactly the
    /// scenario it exists to guard against — a full disk) would have no observable signal at all.
    /// </summary>
    public long FailedTicksTotal => Volatile.Read(ref _failedTicksTotal);

    /// <summary>
    /// Running count of messages forcibly pruned — every increase here is a guarantee-break (a
    /// message discarded regardless of ack status), never routine, expected pruning.
    /// </summary>
    public long MessagesPrunedTotal => Volatile.Read(ref _messagesPrunedTotal);

    /// <summary>
    /// Loops until the actor's mailbox closes (normal shutdown).
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default) {
        var timeProviderOrDefault = timeProvider ?? TimeProvider.System;
        var interval = tickInterval ?? DefaultTickInterval;

        try {
            while (true) {
                await Task.Delay(interval, timeProviderOrDefault, cancellationToken);

                try {
                    if (isAdmissionPaused?.Invoke() ?? false)
                        continue;

                    // Re-evaluated every tick, not resolved once — the real ceiling may not be known
                    // yet at the moment this worker started (see RetentionPolicy.GetMaxTotalBytesPerVirtualShard's
                    // own remarks); a null tick here is a skip, never a permanent disable.
                    if (getMaxTotalBytes() is not { } maxTotalBytes)
                        continue;

                    var deletedSegments = await Compactor.PruneOldestUntilUnderSizeAsync(directory, maxTotalBytes, cancellationToken);
                    if (deletedSegments.Count == 0)
                        continue;

                    var messageCount = deletedSegments.Sum(segment => (long)segment.MessageCount);
                    Interlocked.Add(ref _messagesPrunedTotal, messageCount);
                    // A guarantee break, not routine pruning — always worth a prominent log line,
                    // regardless of ack status (mirrors the Interlocked counter's own remarks).
                    logger?.LogWarning("Force-pruned {MessageCount} messages across {SegmentCount} segments in {Directory} — a guarantee break, not routine pruning.",
                        messageCount, deletedSegments.Count, directory);

                    if (!mailbox.TryWrite(new SizePruneCompleted(deletedSegments.Select(segment => segment.Path).ToArray())))
                        return; // mailbox closed in the meantime — the result is simply discarded, harmless
                } catch (Exception ex) when (!cancellationToken.IsCancellationRequested) {
                    // A transient failure mid-tick must never permanently kill this loop — nothing
                    // else would ever restart it. Best-effort: try again next tick.
                    Interlocked.Increment(ref _failedTicksTotal);
                    logger?.LogWarning(ex, "Size-prune tick failed in {Directory}, will retry next tick.", directory);
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — cancellation can land here whether the loop was idling in
            // Task.Delay or mid-tick inside the prune itself; both must exit cleanly, not fault.
        }
    }
}
