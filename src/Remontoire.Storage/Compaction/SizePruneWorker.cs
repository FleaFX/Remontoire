using System.Threading.Channels;

namespace Remontoire.Storage.Compaction;

/// <summary>
/// Periodically enforces a directory's size-based emergency ceiling, off the actor's own thread.
/// Unlike <see cref="CompactionWorker"/>, no request/response round-trip with the actor is needed
/// first: there's nothing to wait for becoming possible (the ceiling check is always immediately
/// answerable), so this simply ticks, runs <see cref="Compactor.PruneOldestUntilUnderSizeAsync"/>
/// directly, and reports back via <see cref="SizePruneCompleted"/>.
/// </summary>
sealed class SizePruneWorker(string directory, Func<long?> getMaxTotalBytes, Func<bool>? isAdmissionPaused, ChannelWriter<ShardLogMessage> mailbox, TimeProvider? timeProvider = null, TimeSpan? tickInterval = null) {
    static readonly TimeSpan DefaultTickInterval = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Loops until the actor's mailbox closes (normal shutdown).
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default) {
        var timeProviderOrDefault = timeProvider ?? TimeProvider.System;
        var interval = tickInterval ?? DefaultTickInterval;

        while (true) {
            try {
                await Task.Delay(interval, timeProviderOrDefault, cancellationToken);
            } catch (OperationCanceledException) {
                return;
            }

            try {
                if (isAdmissionPaused?.Invoke() ?? false)
                    continue;

                // Re-evaluated every tick, not resolved once — the real ceiling may not be known
                // yet at the moment this worker started (see RetentionPolicy.GetMaxTotalBytesPerVirtualShard's
                // own remarks); a null tick here is a skip, never a permanent disable.
                if (getMaxTotalBytes() is not { } maxTotalBytes)
                    continue;

                var deletedPaths = await Compactor.PruneOldestUntilUnderSizeAsync(directory, maxTotalBytes, cancellationToken);
                if (deletedPaths.Count == 0)
                    continue;

                if (!mailbox.TryWrite(new SizePruneCompleted(deletedPaths)))
                    return; // mailbox closed in the meantime — the result is simply discarded, harmless
            } catch (Exception) when (!cancellationToken.IsCancellationRequested) {
                // A transient failure mid-tick must never permanently kill this loop — nothing
                // else would ever restart it. Best-effort: try again next tick.
            }
        }
    }
}
