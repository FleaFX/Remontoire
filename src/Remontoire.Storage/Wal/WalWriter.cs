using System.Buffers;
using System.Diagnostics;
using System.Threading.Channels;
using Remontoire.Storage.Serialization;

namespace Remontoire.Storage;

/// <summary>
/// Appends <see cref="WalRecord"/>s to a write-ahead log file. Concurrent, in-flight appends
/// are batched into a single fsync per batch ("group commit") instead of one fsync per record.
/// Never touches any in-memory state (no MemTable, no index) — purely a durability primitive.
/// </summary>
public sealed class WalWriter : IAsyncDisposable {
    readonly FileStream _file;
    readonly Channel<PendingAppend> _pending = Channel.CreateUnbounded<PendingAppend>(new UnboundedChannelOptions { SingleReader = true });
    readonly Channel<WalRecord> _committed = Channel.CreateUnbounded<WalRecord>(new UnboundedChannelOptions { SingleReader = true });
    readonly Task _commitLoop;
    readonly Action<TimeSpan>? _onFlushDurationMeasured;

    Exception? _fault;

    // Liveness diagnostic — 0 means "no flush currently in flight", never "in flight since
    // the Unix epoch". Stamped immediately before, and cleared immediately after, the one
    // fsync call below — a stuck flush (not an idle writer) is the only thing this should ever
    // report as unhealthy.
    long _flushInProgressSinceUtcTicks;

    /// <summary>
    /// Wraps an already-open <paramref name="file"/>. Prefer <see cref="OpenAsync"/> for the
    /// common case of opening a WAL file by path; this constructor exists for callers that need
    /// to supply their own already-open <see cref="FileStream"/> (tests, or a caller that owns
    /// file lifetime differently).
    /// </summary>
    public WalWriter(FileStream file, Action<TimeSpan>? onFlushDurationMeasured = null) {
        _file = file;
        _onFlushDurationMeasured = onFlushDurationMeasured;
        _commitLoop = Task.Run(RunCommitLoopAsync);
    }

    /// <summary>
    /// Set immediately before, and cleared immediately after, the batch fsync below —
    /// <see langword="null"/> both when no flush has ever run and when the last one already
    /// finished; only a flush genuinely stuck mid-call reports a non-null, aging value here.
    /// </summary>
    public DateTimeOffset? FlushInProgressSince {
        get {
            var ticks = Volatile.Read(ref _flushInProgressSinceUtcTicks);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    /// <summary>
    /// Yields each record in the exact order it was durably fsynced — the same in-memory
    /// <see cref="WalRecord"/> just written, handed off directly, never re-read from disk. Only
    /// one caller may enumerate this at a time. Named "durable", not "committed": a durable
    /// record can still be truncated away (<see cref="TruncateFromAsync"/>) before a caller's
    /// own notion of "committed" — whatever that means to them — is ever satisfied.
    /// </summary>
    public IAsyncEnumerable<WalRecord> ReadDurableAsync(CancellationToken cancellationToken = default) =>
        _committed.Reader.ReadAllAsync(cancellationToken);

    /// <summary>
    /// Opens (creating if necessary) the WAL file at <paramref name="path"/> for appending.
    /// </summary>
    public static Task<WalWriter> OpenAsync(string path, CancellationToken cancellationToken = default) =>
        OpenAsync(path, onFlushDurationMeasured: null, cancellationToken);

    /// <inheritdoc cref="OpenAsync(string, CancellationToken)"/>
    /// <param name="path">The WAL file path.</param>
    /// <param name="onFlushDurationMeasured">
    /// Invoked once per batch fsync with its wall-clock duration — the source for a fsync-duration
    /// metric maintained outside this project. This project never references a metrics library
    /// directly (same one-way dependency discipline as
    /// <see cref="CompactionPolicy.GetAckedLowWatermarkAsync"/>); <see langword="null"/> disables
    /// the callback entirely.
    /// </param>
    /// <param name="cancellationToken">Unused today — reserved for a future async open path.</param>
    public static Task<WalWriter> OpenAsync(string path, Action<TimeSpan>? onFlushDurationMeasured, CancellationToken cancellationToken = default) =>
        Task.FromResult(new WalWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 0, useAsync: true), onFlushDurationMeasured));

    /// <summary>
    /// Appends <paramref name="record"/>. The returned <see cref="ValueTask"/> completes once
    /// the record — together with the rest of its commit batch — is durable (fsynced), never
    /// before. Awaiting with <paramref name="cancellationToken"/> cancelled stops the caller
    /// from waiting, but does not abort the underlying write; it will still be durably written.
    /// </summary>
    public ValueTask AppendAsync(WalRecord record, CancellationToken cancellationToken = default) {
        if (Volatile.Read(ref _fault) is { } fault)
            return ValueTask.FromException(fault);

        var pending = new PendingAppend(record, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        var accepted = _pending.Writer.TryWrite(pending);
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask(pending.Completion.Task.WaitAsync(cancellationToken));
    }

    /// <summary>
    /// Discards every byte from <paramref name="position"/> onward, permanently. Caller
    /// contract: no <see cref="AppendAsync"/> may be in flight when this is called — the only
    /// real caller is driven by a single-threaded actor loop that always awaits one WAL
    /// operation to completion before issuing the next, so this is enforced by construction,
    /// not by a runtime guard here.
    /// </summary>
    public Task TruncateFromAsync(long position, CancellationToken cancellationToken = default) {
        if (Volatile.Read(ref _fault) is { } fault)
            return Task.FromException(fault);

        try {
            _file.SetLength(position);
            _file.Position = position;
        } catch (Exception ex) {
            var truncateFault = new IOException("WalWriter could not truncate and is now permanently unusable.", ex);
            Volatile.Write(ref _fault, truncateFault);
            return Task.FromException(truncateFault);
        }

        return Task.CompletedTask;
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

            if (Volatile.Read(ref _fault) is not null)
                break; // unrecoverable — stop processing further batches
        }

        // A permanent fault can leave items sitting in `_pending` that arrived just before
        // AppendAsync started rejecting new calls — resolve them too, so nothing is left hanging.
        if (Volatile.Read(ref _fault) is { } fault)
            while (reader.TryRead(out var pending))
                pending.Completion.TrySetException(fault);

        _committed.Writer.TryComplete();
    }

    async Task WriteBatchAsync(List<PendingAppend> batch) {
        var positionBeforeBatch = _file.Position;

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

            Volatile.Write(ref _flushInProgressSinceUtcTicks, DateTimeOffset.UtcNow.Ticks);
            try {
                if (_onFlushDurationMeasured is null) {
                    _file.Flush(flushToDisk: true);
                } else {
                    var stopwatch = Stopwatch.StartNew();
                    _file.Flush(flushToDisk: true);
                    _onFlushDurationMeasured(stopwatch.Elapsed);
                }
            } finally {
                Volatile.Write(ref _flushInProgressSinceUtcTicks, 0);
            }

            // Publish each record — the same in-memory WalRecord just written, handed off
            // directly to ReadDurableAsync — right after resolving its caller, in the exact
            // order this single, sequential commit loop wrote them. No re-read from disk, and
            // no risk of two records ever being observed out of order: this loop is the one and
            // only place that decides "durable" order in the first place.
            foreach (var pending in batch) {
                pending.Completion.TrySetResult();
                _committed.Writer.TryWrite(pending.Record);
            }
        } catch (Exception ex) {
            // Undo whatever this batch already wrote — otherwise a later, successful batch's
            // flush would fsync these bytes too, silently making a failed append durable anyway.
            try {
                _file.SetLength(positionBeforeBatch);
                _file.Position = positionBeforeBatch;
            } catch (Exception truncateEx) {
                Volatile.Write(ref _fault, new IOException(
                    "WalWriter could not recover from a write failure and is now permanently unusable.",
                    new AggregateException(ex, truncateEx))
                );
            }

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
