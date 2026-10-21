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
    ulong _appliedIndex;

    /// <summary>
    /// Starts forwarding immediately — every record already applied replays first, same as any other reader of <see cref="ShardLog.ReadAppliedAsync"/>.
    /// </summary>
    public AckIndexApplier(ShardLog shardLog, AckIndex ackIndex) =>
        _loop = Task.Run(() => RunAsync(shardLog, ackIndex, _cts.Token));

    /// <summary>
    /// The <see cref="WalRecord.RaftIndex"/> of the last record applied, whatever its type; zero
    /// before the first. Once it reaches the replica's commit index, the ack index reflects every
    /// committed ack — what the embedded engine waits for at startup before it serves consumers.
    /// </summary>
    public ulong AppliedIndex => Volatile.Read(ref _appliedIndex);

    async Task RunAsync(ShardLog shardLog, AckIndex ackIndex, CancellationToken cancellationToken) {
        try {
            await foreach (var record in shardLog.ReadAppliedAsync(cancellationToken)) {
                await ackIndex.ApplyAsync(record, cancellationToken);
                Volatile.Write(ref _appliedIndex, record.RaftIndex);
            }
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
