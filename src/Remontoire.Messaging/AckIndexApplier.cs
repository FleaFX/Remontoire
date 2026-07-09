using Remontoire.Storage;

namespace Remontoire.Messaging;

/// <summary>
/// Consumes <see cref="ShardLog.ReadAppliedAsync"/> and applies every record to <see cref="AckIndex"/>
/// — a pure forwarder, same shape as <see cref="ShardLog"/>'s own committed-source tailing loop.
/// Owns nothing but the loop itself; the shard log and ack index it's given are both owned by,
/// and outlive, their caller.
/// </summary>
public sealed class AckIndexApplier : IAsyncDisposable {
    readonly CancellationTokenSource _cts = new();
    readonly Task _loop;

    /// <summary>
    /// Starts forwarding immediately — every record already applied replays first, same as any other reader of <see cref="ShardLog.ReadAppliedAsync"/>.
    /// </summary>
    public AckIndexApplier(ShardLog shardLog, AckIndex ackIndex) =>
        _loop = Task.Run(() => RunAsync(shardLog, ackIndex, _cts.Token));

    static async Task RunAsync(ShardLog shardLog, AckIndex ackIndex, CancellationToken cancellationToken) {
        try {
            await foreach (var record in shardLog.ReadAppliedAsync(cancellationToken))
                ackIndex.Apply(record);
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
        }
    }

    /// <summary>
    /// Stops the forwarding loop and awaits its shutdown.
    /// </summary>
    public async ValueTask DisposeAsync() {
        await _cts.CancelAsync();
        await _loop;
        _cts.Dispose();
    }
}
