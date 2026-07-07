using System.Buffers;
using System.Threading.Channels;
using Remontoire.Storage.Serialization;

namespace Remontoire.Storage;

/// <summary>
/// Appends <see cref="WalRecord"/>s to a write-ahead log file. Concurrent, in-flight appends
/// are batched into a single fsync per batch ("group commit") instead of one fsync per record.
/// Never touches any in-memory state (no MemTable, no index) — purely a durability primitive.
/// </summary>
sealed class WalWriter : IAsyncDisposable {
    readonly FileStream _file;
    readonly Channel<PendingAppend> _pending = Channel.CreateUnbounded<PendingAppend>(new UnboundedChannelOptions { SingleReader = true });
    readonly Task _commitLoop;

    internal WalWriter(FileStream file) {
        _file = file;
        _commitLoop = Task.Run(RunCommitLoopAsync);
    }

    /// <summary>
    /// Opens (creating if necessary) the WAL file at <paramref name="path"/> for appending.
    /// </summary>
    public static Task<WalWriter> OpenAsync(string path, CancellationToken cancellationToken = default) =>
        Task.FromResult(new WalWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 0, useAsync: true)));

    /// <summary>
    /// Appends <paramref name="record"/>. The returned <see cref="ValueTask"/> completes once
    /// the record — together with the rest of its commit batch — is durable (fsynced), never
    /// before. Awaiting with <paramref name="cancellationToken"/> cancelled stops the caller
    /// from waiting, but does not abort the underlying write; it will still be durably written.
    /// </summary>
    public ValueTask AppendAsync(WalRecord record, CancellationToken cancellationToken = default) {
        var pending = new PendingAppend(record, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        var accepted = _pending.Writer.TryWrite(pending);
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask(pending.Completion.Task.WaitAsync(cancellationToken));
    }

    async Task RunCommitLoopAsync() {
        var reader = _pending.Reader;
        var batch = new List<PendingAppend>();

        while (await reader.WaitToReadAsync()) {
            batch.Clear();
            while (reader.TryRead(out var pending))
                batch.Add(pending);

            if (batch.Count > 0)
                await WriteBatchAsync(batch);
        }
    }

    async Task WriteBatchAsync(List<PendingAppend> batch) {
        try {
            foreach (var pending in batch) {
                var length = WalRecordSerializer.GetEncodedLength(pending.Record);
                var buffer = ArrayPool<byte>.Shared.Rent(length);
                try {
                    var written = WalRecordSerializer.Write(pending.Record, buffer);
                    await _file.WriteAsync(buffer.AsMemory(0, written));
                } finally {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }

            _file.Flush(flushToDisk: true);

            foreach (var pending in batch)
                pending.Completion.TrySetResult();
        } catch (Exception ex) {
            foreach (var pending in batch)
                pending.Completion.TrySetException(ex);
        }
    }

    /// <summary>
    /// Stops accepting new appends and waits for everything already queued to be durably
    /// written before returning. Never rejects already-queued appends — this is a graceful,
    /// drain-to-completion shutdown, not a cancellation.
    /// </summary>
    public async ValueTask DisposeAsync() {
        _pending.Writer.TryComplete();
        await _commitLoop;
        await _file.DisposeAsync();
    }

    readonly record struct PendingAppend(WalRecord Record, TaskCompletionSource Completion);
}
