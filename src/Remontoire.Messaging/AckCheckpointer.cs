namespace Remontoire.Messaging;

/// <summary>
/// Periodically proposes a cheap AckCheckpoint record replicating each checkpoint-mode consumer
/// group's current watermark — the periodic counterpart to <see cref="AckIndex.ApplyLocal"/>'s
/// immediate, unreplicated apply. Takes every cross-layer fact as a delegate over primitives —
/// never a <c>Remontoire.Raft</c> or <c>Remontoire.Sharding</c> type — so this project's
/// dependency graph (only <c>Remontoire.Storage</c>) stays unchanged. Same start/stop lifecycle
/// discipline as <see cref="AckIndexApplier"/>.
/// </summary>
public sealed class AckCheckpointer : IAsyncDisposable {
    static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(1);

    readonly CancellationTokenSource _cts = new();
    readonly Task _loop;

    /// <summary>
    /// Starts ticking immediately. A no-op tick when <paramref name="isLeader"/> returns
    /// <see langword="false"/> (only the leader may propose), when <paramref name="isAdmissionPaused"/>
    /// returns <see langword="true"/> (mid-reshard-pause), or when no group's watermark has
    /// advanced past its configured interval since the last checkpoint for it.
    /// </summary>
    public AckCheckpointer(
        AckIndex ackIndex, Func<string, ulong, CancellationToken, Task> proposeCheckpointAsync,
        Func<bool> isLeader, Func<string, bool> isCheckpointMode,
        Func<(TimeSpan? Interval, int? OffsetCount)> getCheckpointThresholds,
        Func<bool> isAdmissionPaused, TimeProvider? timeProvider = null) =>
        _loop = Task.Run(() => RunAsync(ackIndex, proposeCheckpointAsync, isLeader, isCheckpointMode, getCheckpointThresholds, isAdmissionPaused, timeProvider ?? TimeProvider.System, _cts.Token));

    static async Task RunAsync(
        AckIndex ackIndex, Func<string, ulong, CancellationToken, Task> proposeCheckpointAsync,
        Func<bool> isLeader, Func<string, bool> isCheckpointMode,
        Func<(TimeSpan? Interval, int? OffsetCount)> getCheckpointThresholds,
        Func<bool> isAdmissionPaused, TimeProvider timeProvider, CancellationToken cancellationToken) {
        var lastCheckpointed = new Dictionary<string, (ulong Watermark, DateTimeOffset At)>();

        try {
            while (true) {
                await Task.Delay(TickInterval, timeProvider, cancellationToken);

                if (!isLeader() || isAdmissionPaused())
                    continue;

                var (interval, offsetCount) = getCheckpointThresholds();
                foreach (var consumerGroup in ackIndex.RegisteredConsumerGroups()) {
                    if (!isCheckpointMode(consumerGroup))
                        continue;

                    // LowWatermark ("applied"), deliberately not CommittedWatermark — this is
                    // exactly the value that's still waiting to become committed; that's this
                    // component's own job to propose.
                    var watermark = ackIndex.GetOrCreate(consumerGroup).LowWatermark;
                    var (lastWatermark, lastAt) = lastCheckpointed.GetValueOrDefault(consumerGroup, (0UL, DateTimeOffset.MinValue));
                    if (watermark <= lastWatermark)
                        continue; // nothing new to checkpoint for this group

                    var dueByCount = offsetCount is { } count && watermark - lastWatermark >= (ulong)count;
                    var dueByTime = interval is { } iv && timeProvider.GetUtcNow() - lastAt >= iv;
                    if (!dueByCount && !dueByTime)
                        continue;

                    await proposeCheckpointAsync(consumerGroup, watermark, cancellationToken);
                    lastCheckpointed[consumerGroup] = (watermark, timeProvider.GetUtcNow());
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
