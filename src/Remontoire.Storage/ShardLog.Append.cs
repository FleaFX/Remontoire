namespace Remontoire.Storage;

public sealed partial class ShardLog {
    /// <summary>
    /// Appends <paramref name="request"/> and returns its assigned <c>LogicalOffset</c> once
    /// durable (fsynced) — not necessarily yet visible via <see cref="TryGet"/>/
    /// <see cref="ReadFromAsync"/>, which lag slightly behind (the live-apply tailing loop).
    /// </summary>
    public ValueTask<ulong> AppendAsync(AppendRequest request, CancellationToken cancellationToken = default) {
        var completion = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _mailbox.Writer.TryWrite(new AppendCommand(request, completion));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<ulong>(completion.Task.WaitAsync(cancellationToken));
    }

    // Runs on the actor loop's single thread (ShardLog.cs's RunActorAsync) — assigning the
    // offset and enqueuing to WalWriter happen atomically w.r.t. every other message, so
    // concurrent AppendAsync callers can never end up interleaved out of order. Durability
    // itself is NOT awaited here — that would serialize every append behind its own fsync and
    // defeat WalWriter's group commit entirely.
    void HandleAppend(AppendCommand command) {
        var record = new WalRecord(WalRecordType.Append, RaftTerm: 0, RaftIndex: 0, _nextLogicalOffset++, (ulong)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000),
            command.Request.PartitionKey, command.Request.Headers, command.Request.Payload);

        var durable = _walWriter.AppendAsync(record); // synchronous enqueue — not awaited here
        if (durable.IsCompletedSuccessfully)
            command.Completion.TrySetResult(record.LogicalOffset);
        else
            _ = ForwardAsync(durable, command.Completion, record.LogicalOffset);

        return;

        static async Task ForwardAsync(ValueTask durable, TaskCompletionSource<ulong> completion, ulong offset) {
            try {
                await durable;
                completion.TrySetResult(offset);
            } catch (Exception ex) {
                completion.TrySetException(ex);
            }
        }
    }
}
