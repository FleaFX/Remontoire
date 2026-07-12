namespace Remontoire.Messaging;

/// <summary>
/// Everything <see cref="AckCheckpointer"/> needs, bundled into one type instead of a long,
/// duplicated parameter list — same "Options" shape used by several other composition roots
/// elsewhere in this codebase.
/// </summary>
public sealed record AckCheckpointerOptions(
    AckIndex AckIndex, Func<string, ulong, CancellationToken, Task> ProposeCheckpointAsync,
    Func<bool> IsLeader, Func<string, bool> IsCheckpointMode,
    Func<(TimeSpan? Interval, int? OffsetCount)> GetCheckpointThresholds,
    Func<bool> IsAdmissionPaused, TimeProvider? TimeProvider = null);

/// <summary>
/// Periodically proposes a cheap AckCheckpoint record replicating each checkpoint-mode consumer
/// group's current watermark — the periodic counterpart to <see cref="AckIndex.ApplyLocalAsync"/>'s
/// immediate, unreplicated apply. Takes every cross-layer fact as a delegate over primitives —
/// never a <c>Remontoire.Raft</c> or <c>Remontoire.Sharding</c> type — so this project's
/// dependency graph (only <c>Remontoire.Storage</c>) stays unchanged. Same start/stop lifecycle
/// discipline as <see cref="AckIndexApplier"/>.
/// </summary>
public sealed class AckCheckpointer : IAsyncDisposable {
    static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(1);
    // (Interval: null, OffsetCount: null) must not mean "never checkpoint" — without it, a
    // checkpoint-mode group that never gets an explicit SetStreamCheckpointInterval would never
    // checkpoint at all, permanently blocking pruning (if mandatory) and losing ALL ack progress
    // on any failover, not just "up to one interval's worth" as the mode's own contract promises.
    static readonly TimeSpan DefaultCheckpointInterval = TimeSpan.FromSeconds(30);

    readonly CancellationTokenSource _cts = new();
    readonly Task _loop;

    /// <summary>
    /// Starts ticking immediately. A no-op tick when <see cref="AckCheckpointerOptions.IsLeader"/>
    /// returns <see langword="false"/> (only the leader may propose), when
    /// <see cref="AckCheckpointerOptions.IsAdmissionPaused"/> returns <see langword="true"/>
    /// (mid-reshard-pause), or when no group's watermark has advanced past its configured
    /// interval since the last checkpoint for it.
    /// </summary>
    public AckCheckpointer(AckCheckpointerOptions options) =>
        _loop = Task.Run(() => RunAsync(options, _cts.Token));

    // Stays static, unlike RetentionEvaluator's own RunAsync: this one touches no instance state
    // of its own, so passing the options bundle straight through keeps every dependency explicit
    // without an implicit `this` capture — same discipline AckIndexApplier's RunAsync already follows.
    static async Task RunAsync(AckCheckpointerOptions options, CancellationToken cancellationToken) {
        var timeProvider = options.TimeProvider ?? TimeProvider.System;
        var lastCheckpointed = new Dictionary<string, (ulong Watermark, DateTimeOffset At)>();

        try {
            while (true) {
                await Task.Delay(TickInterval, timeProvider, cancellationToken);

                if (!options.IsLeader() || options.IsAdmissionPaused())
                    continue;

                var (interval, offsetCount) = options.GetCheckpointThresholds();
                // Only fall back when NEITHER threshold is set — an operator who explicitly chose
                // a count-only threshold must not also get a silent, unrequested time-based one.
                var effectiveInterval = interval ?? (offsetCount is null ? DefaultCheckpointInterval : null);
                foreach (var consumerGroup in options.AckIndex.RegisteredConsumerGroups()) {
                    if (!options.IsCheckpointMode(consumerGroup))
                        continue;

                    // LowWatermark ("applied"), deliberately not CommittedWatermark — this is
                    // exactly the value that's still waiting to become committed; that's this
                    // component's own job to propose.
                    var watermark = options.AckIndex.GetOrCreate(consumerGroup).LowWatermark;
                    var (lastWatermark, lastAt) = lastCheckpointed.GetValueOrDefault(consumerGroup, (0UL, DateTimeOffset.MinValue));
                    if (watermark <= lastWatermark)
                        continue; // nothing new to checkpoint for this group

                    var dueByCount = offsetCount is { } count && watermark - lastWatermark >= (ulong)count;
                    var dueByTime = effectiveInterval is { } iv && timeProvider.GetUtcNow() - lastAt >= iv;
                    if (!dueByCount && !dueByTime)
                        continue;

                    try {
                        await options.ProposeCheckpointAsync(consumerGroup, watermark, cancellationToken);
                        lastCheckpointed[consumerGroup] = (watermark, timeProvider.GetUtcNow());
                    } catch (Exception) when (!cancellationToken.IsCancellationRequested) {
                        // A transient failure for this one group (e.g. a leadership handover mid-
                        // propose) must never stop the others in this tick, or permanently kill
                        // the loop — nothing else would ever restart it. Retried next tick.
                    }
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
        }
    }

    /// <summary>
    /// Stops the checkpoint loop and awaits its shutdown.
    /// </summary>
    public async ValueTask DisposeAsync() {
        await _cts.CancelAsync();
        await _loop;
        _cts.Dispose();
    }
}
