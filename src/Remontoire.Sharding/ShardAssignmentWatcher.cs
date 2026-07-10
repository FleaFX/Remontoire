using Grpc.Core;
using Remontoire.Meta.V1;

namespace Remontoire.Sharding;

/// <summary>
/// The over-the-wire counterpart to a direct, in-process applier: fills a <see cref="ShardAssignmentTable"/>
/// from a remote meta-group member instead of a local committed-record stream, for any consumer
/// that doesn't itself host a meta-group replica — every server node without meta membership, and
/// every client connection. Two, together-redundant mechanisms keep the table fresh: an initial
/// <c>GetSnapshot</c> call plus a continuous <c>Watch</c> live tail (primary, near-zero latency),
/// and a low-frequency periodic <c>GetSnapshot</c> reconciliation poll (defense-in-depth, in case a
/// <c>Watch</c> stream silently stalls without visibly failing).
/// </summary>
public sealed class ShardAssignmentWatcher : IAsyncDisposable {
    static readonly TimeSpan DefaultReconciliationInterval = TimeSpan.FromMinutes(2);
    static readonly TimeSpan WatchReconnectDelay = TimeSpan.FromMilliseconds(200);

    readonly CancellationTokenSource _cts = new();
    readonly Task _watchLoop;
    readonly Task _reconciliationLoop;

    // Guards against the live Watch tail and the periodic reconciliation poll racing each other:
    // GetSnapshot always returns the entire history, not just what's new, so a reconciliation
    // call in flight while Watch delivers a newer record could otherwise replay stale state on
    // top of it. Applying only ever moves this forward, and only while holding the gate for the
    // whole check-then-apply, so the two loops can never apply out of version order.
    readonly Lock _versionGate = new();
    ulong? _lastAppliedVersion; // null: nothing applied yet — version 0 (a real, valid first record) must still apply

    /// <summary>
    /// Starts filling <paramref name="table"/> immediately: an initial <c>GetSnapshot</c>, then a
    /// continuous <c>Watch</c> from the version it returned, alongside a periodic reconciliation
    /// poll every <paramref name="reconciliationInterval"/> (default two minutes).
    /// </summary>
    public ShardAssignmentWatcher(ShardAssignmentMeta.ShardAssignmentMetaClient client, ShardAssignmentTable table, TimeSpan? reconciliationInterval = null) {
        _watchLoop = Task.Run(() => RunWatchLoopAsync(client, table, _cts.Token));
        _reconciliationLoop = Task.Run(() => RunReconciliationLoopAsync(client, table, reconciliationInterval ?? DefaultReconciliationInterval, _cts.Token));
    }

    async Task RunWatchLoopAsync(ShardAssignmentMeta.ShardAssignmentMetaClient client, ShardAssignmentTable table, CancellationToken cancellationToken) {
        try {
            var fromVersion = 0UL;
            while (true) {
                try {
                    var snapshot = await client.GetSnapshotAsync(new GetSnapshotRequest(), cancellationToken: cancellationToken);
                    foreach (var record in snapshot.Records)
                        ApplyIfNewer(table, record);

                    fromVersion = snapshot.Version;
                    break;
                } catch (RpcException) {
                    // The meta-group isn't reachable yet (e.g. still starting up) — keep retrying
                    // the initial fill rather than giving up on the whole watcher.
                    await Task.Delay(WatchReconnectDelay, cancellationToken);
                }
            }

            while (true) {
                try {
                    using var call = client.Watch(new WatchRequest { FromVersion = fromVersion }, cancellationToken: cancellationToken);
                    while (await call.ResponseStream.MoveNext(cancellationToken)) {
                        var record = call.ResponseStream.Current;
                        ApplyIfNewer(table, record);
                        fromVersion = record.Version;
                    }
                } catch (RpcException) {
                    // A dropped stream or an unreachable member — reconnect and resume from
                    // wherever we last got to; the periodic reconciliation poll is the backstop
                    // if this loop somehow stops making progress for longer than expected.
                    await Task.Delay(WatchReconnectDelay, cancellationToken);
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
        }
    }

    async Task RunReconciliationLoopAsync(ShardAssignmentMeta.ShardAssignmentMetaClient client, ShardAssignmentTable table, TimeSpan interval, CancellationToken cancellationToken) {
        try {
            while (true) {
                await Task.Delay(interval, cancellationToken);

                try {
                    var snapshot = await client.GetSnapshotAsync(new GetSnapshotRequest(), cancellationToken: cancellationToken);
                    foreach (var record in snapshot.Records)
                        ApplyIfNewer(table, record);
                } catch (RpcException) {
                    // Best-effort — the next scheduled poll, or the live Watch loop, will catch up.
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
        }
    }

    // Applying and advancing the gate together, under the same lock, is what keeps the two loops
    // from ever landing an older record after a newer one already won for the same key.
    void ApplyIfNewer(ShardAssignmentTable table, MetaLogRecordProto record) {
        lock (_versionGate) {
            if (_lastAppliedVersion is { } lastApplied && record.Version <= lastApplied)
                return;

            table.Apply(MetaLogRecord.Decode(record.Payload.Span));
            _lastAppliedVersion = record.Version;
        }
    }

    /// <summary>
    /// Stops both loops and awaits their shutdown.
    /// </summary>
    public async ValueTask DisposeAsync() {
        await _cts.CancelAsync();
        await Task.WhenAll(_watchLoop, _reconciliationLoop);
        _cts.Dispose();
    }
}
