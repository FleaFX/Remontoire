using Remontoire.Storage;

namespace Remontoire.Messaging;

/// <summary>
/// Everything <see cref="RetentionEvaluator"/> needs, bundled into one type instead of a long,
/// duplicated parameter list — same "Options" shape as <c>RaftServerOptions</c>/<c>RemontoireClientOptions</c>
/// elsewhere in this codebase.
/// </summary>
public sealed record RetentionEvaluatorOptions(
    ShardLog ShardLog, AckIndex AckIndex, Func<string, bool> IsMandatory,
    Func<TimeSpan> GetMaxRetention, Func<AppendRequest, CancellationToken, Task> ForwardToDeadLetterAsync,
    Func<bool> IsAdmissionPaused, TimeProvider? TimeProvider = null, TimeSpan? TickInterval = null);

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
    static readonly TimeSpan DefaultTickInterval = TimeSpan.FromMinutes(1);

    readonly RetentionEvaluatorOptions _options;
    readonly TimeProvider _timeProvider;
    readonly TimeSpan _tickInterval;
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
    /// Starts ticking immediately. A no-op tick when <see cref="RetentionEvaluatorOptions.IsAdmissionPaused"/>
    /// returns <see langword="true"/> (mid-reshard-pause).
    /// </summary>
    public RetentionEvaluator(RetentionEvaluatorOptions options) {
        _options = options;
        _timeProvider = options.TimeProvider ?? TimeProvider.System;
        _tickInterval = options.TickInterval ?? DefaultTickInterval;
        _loop = Task.Run(() => RunAsync(_cts.Token));
    }

    // Not static, unlike AckIndexApplier/AckCheckpointer's own RunAsync — this one already reads
    // and writes instance state (_safeToPruneWatermark/_deadLetterMessagesTotal) directly, so
    // storing _options (set once, above) avoids redeclaring its own parameter list a second time.
    async Task RunAsync(CancellationToken cancellationToken) {
        try {
            while (true) {
                await Task.Delay(_tickInterval, _timeProvider, cancellationToken);

                try {
                    if (_options.IsAdmissionPaused())
                        continue;

                    var mandatoryWatermark = _options.AckIndex.MandatoryGroupsLowWatermark(_options.IsMandatory);
                    var maxRetention = _options.GetMaxRetention();
                    var now = _timeProvider.GetUtcNow();
                    var cutoff = now - TimeSpan.FromTicks(Math.Clamp(maxRetention.Ticks, TimeSpan.Zero.Ticks, (now - DateTimeOffset.MinValue).Ticks));
                    var scanFrom = SafeToPruneWatermark;

                    await foreach (var handle in _options.ShardLog.ReadFromAsync(scanFrom, cancellationToken)) {
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
                            await _options.ForwardToDeadLetterAsync(new AppendRequest(entry.PartitionKey, entry.Headers, entry.Payload), cancellationToken);
                            Interlocked.Increment(ref _deadLetterMessagesTotal);
                            Volatile.Write(ref _safeToPruneWatermark, entry.LogicalOffset + 1);
                        }
                    }
                } catch (Exception) when (!cancellationToken.IsCancellationRequested) {
                    // A transient failure mid-tick (e.g. a leadership handover while forwarding)
                    // must never permanently kill this loop — nothing else would ever restart it.
                    // Best-effort: try again next tick.
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
