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

    /// <summary>
    /// Starts filling <paramref name="table"/> immediately: an initial <c>GetSnapshot</c>, then a
    /// continuous <c>Watch</c> from the version it returned, alongside a periodic reconciliation
    /// poll every <paramref name="reconciliationInterval"/> (default two minutes).
    /// </summary>
    public ShardAssignmentWatcher(ShardAssignmentMeta.ShardAssignmentMetaClient client, ShardAssignmentTable table, TimeSpan? reconciliationInterval = null) {
        _watchLoop = Task.Run(() => RunWatchLoopAsync(client, table, _cts.Token));
        _reconciliationLoop = Task.Run(() => RunReconciliationLoopAsync(client, table, reconciliationInterval ?? DefaultReconciliationInterval, _cts.Token));
    }

    static async Task RunWatchLoopAsync(ShardAssignmentMeta.ShardAssignmentMetaClient client, ShardAssignmentTable table, CancellationToken cancellationToken) {
        try {
            var snapshot = await client.GetSnapshotAsync(new GetSnapshotRequest(), cancellationToken: cancellationToken);
            foreach (var record in snapshot.Records)
                table.Apply(MetaLogRecord.Decode(record.Payload.Span));

            var fromVersion = snapshot.Version;
            while (true) {
                try {
                    using var call = client.Watch(new WatchRequest { FromVersion = fromVersion }, cancellationToken: cancellationToken);
                    while (await call.ResponseStream.MoveNext(cancellationToken)) {
                        var record = call.ResponseStream.Current;
                        table.Apply(MetaLogRecord.Decode(record.Payload.Span));
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

    static async Task RunReconciliationLoopAsync(ShardAssignmentMeta.ShardAssignmentMetaClient client, ShardAssignmentTable table, TimeSpan interval, CancellationToken cancellationToken) {
        try {
            while (true) {
                await Task.Delay(interval, cancellationToken);

                try {
                    var snapshot = await client.GetSnapshotAsync(new GetSnapshotRequest(), cancellationToken: cancellationToken);
                    foreach (var record in snapshot.Records)
                        table.Apply(MetaLogRecord.Decode(record.Payload.Span));
                } catch (RpcException) {
                    // Best-effort — the next scheduled poll, or the live Watch loop, will catch up.
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
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
