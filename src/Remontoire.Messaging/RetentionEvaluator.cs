using Remontoire.Storage;

namespace Remontoire.Messaging;

/// <summary>
/// Periodically scans for messages past their stream's max-retention window that no mandatory
/// consumer group has yet acked, forwards each to the dead-letter stream, then folds the result
/// into <see cref="SafeToPruneWatermark"/> — the single number a caller wires into
/// <see cref="CompactionPolicy.GetAckedLowWatermarkAsync"/>, so <see cref="Remontoire.Storage"/>
/// never needs to know whether a given offset became prunable via a real ack or via forced
/// dead-lettering — both simply advance the same watermark. Reuses <see cref="ShardLog.ReadFromAsync"/>
/// (already public, already used by consume paths) to walk candidate entries — never re-derives or
/// duplicates message content of its own.
/// </summary>
public sealed class RetentionEvaluator : IAsyncDisposable {
    static readonly TimeSpan TickInterval = TimeSpan.FromMinutes(1);

    readonly CancellationTokenSource _cts = new();
    readonly Task _loop;
    ulong _safeToPruneWatermark;
    long _deadLetterMessagesTotal;

    /// <summary>
    /// The combined watermark described above — everything below it is safe to prune.
    /// </summary>
    public ulong SafeToPruneWatermark => Volatile.Read(ref _safeToPruneWatermark);

    /// <summary>
    /// Running count of messages forwarded to the dead-letter stream.
    /// </summary>
    public long DeadLetterMessagesTotal => Volatile.Read(ref _deadLetterMessagesTotal);

    /// <summary>
    /// Starts ticking immediately. A no-op tick when <paramref name="isAdmissionPaused"/> returns
    /// <see langword="true"/> (mid-reshard-pause).
    /// </summary>
    public RetentionEvaluator(
        ShardLog shardLog, AckIndex ackIndex, Func<string, bool> isMandatory,
        Func<TimeSpan> getMaxRetention, Func<AppendRequest, CancellationToken, Task> forwardToDeadLetterAsync,
        Func<bool> isAdmissionPaused, TimeProvider? timeProvider = null) =>
        _loop = Task.Run(() => RunAsync(shardLog, ackIndex, isMandatory, getMaxRetention, forwardToDeadLetterAsync, isAdmissionPaused, timeProvider ?? TimeProvider.System, _cts.Token));

    async Task RunAsync(
        ShardLog shardLog, AckIndex ackIndex, Func<string, bool> isMandatory,
        Func<TimeSpan> getMaxRetention, Func<AppendRequest, CancellationToken, Task> forwardToDeadLetterAsync,
        Func<bool> isAdmissionPaused, TimeProvider timeProvider, CancellationToken cancellationToken) {
        try {
            while (true) {
                await Task.Delay(TickInterval, timeProvider, cancellationToken);
                if (isAdmissionPaused())
                    continue;

                var mandatoryWatermark = ackIndex.MandatoryGroupsLowWatermark(isMandatory);
                var cutoff = timeProvider.GetUtcNow() - getMaxRetention();
                var scanFrom = SafeToPruneWatermark;

                await foreach (var handle in shardLog.ReadFromAsync(scanFrom, cancellationToken)) {
                    using (handle) {
                        var entry = handle.Entry;
                        if (entry.LogicalOffset < mandatoryWatermark) {
                            // Already acked by every mandatory group — safe, no forcing needed.
                            Volatile.Write(ref _safeToPruneWatermark, entry.LogicalOffset + 1);
                            continue;
                        }

                        if (DateTimeOffset.FromUnixTimeMilliseconds((long)(entry.TimestampMicros / 1000)) > cutoff)
                            break; // this entry, and everything after it, is still within the max-retention window

                        // Past max-retention, still not fully acked by a mandatory group — force it
                        // through the dead-letter stream before letting the watermark cover it.
                        await forwardToDeadLetterAsync(new AppendRequest(entry.PartitionKey, entry.Headers, entry.Payload), cancellationToken);
                        Interlocked.Increment(ref _deadLetterMessagesTotal);
                        Volatile.Write(ref _safeToPruneWatermark, entry.LogicalOffset + 1);
                    }
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
        }
    }

    /// <summary>
    /// Stops the retention loop and awaits its shutdown.
    /// </summary>
    public async ValueTask DisposeAsync() {
        await _cts.CancelAsync();
        await _loop;
        _cts.Dispose();
    }
}
