namespace Remontoire.Storage;

public sealed partial class ShardLog {
    /// <summary>
    /// Appends <paramref name="request"/> and returns its assigned <c>LogicalOffset</c> once
    /// durable (fsynced) — not necessarily yet visible via <see cref="TryGet"/>/
    /// <see cref="ReadFromAsync"/>, which lag slightly behind (the live-apply tailing loop).
    /// </summary>
    public ValueTask<ulong> AppendAsync(AppendRequest request, CancellationToken cancellationToken = default) {
        ValidateRequest(request);

        var completion = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _mailbox.Writer.TryWrite(new AppendCommand(request, completion));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<ulong>(completion.Task.WaitAsync(cancellationToken));
    }

    // PartitionKey/header keys are encoded with a 16-bit length prefix on disk (WalRecordSerializer,
    // via VariableLengthTail) — validated here, at the public API boundary, rather than deep in the
    // serializer or on the actor loop: an exception escaping HandleAppend would crash _actorLoop
    // itself (nobody observes that Task until DisposeAsync), hanging every future append instead
    // of failing this one cleanly. Payload/header values use a 32-bit prefix (~4 GB) — not
    // validated, an unreachable scenario in practice.
    static void ValidateRequest(AppendRequest request) {
        if (request.PartitionKey.Length > ushort.MaxValue)
            throw new ArgumentException($"PartitionKey is {request.PartitionKey.Length} bytes, exceeds the 16-bit length-prefix limit of {ushort.MaxValue}.", nameof(request));

        if (request.Headers.Count > ushort.MaxValue)
            throw new ArgumentException($"Request has {request.Headers.Count} headers, exceeds the 16-bit count-prefix limit of {ushort.MaxValue}.", nameof(request));

        foreach (var header in request.Headers) {
            if (header.Key.Length > ushort.MaxValue)
                throw new ArgumentException($"A header key is {header.Key.Length} bytes, exceeds the 16-bit length-prefix limit of {ushort.MaxValue}.", nameof(request));
        }
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
