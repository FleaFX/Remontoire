using System.Runtime.CompilerServices;
using Remontoire.Storage;
using Remontoire.Storage.Serialization;

namespace Remontoire.Raft;

/// <summary>
/// The production <see cref="IRaftLog"/> — owns one <see cref="WalWriter"/>/<see cref="WalReader"/>
/// pair over a single, unbounded <c>wal.log</c> file, and a live-maintained
/// <c>RaftIndex → byte position</c> index so <see cref="GetTermAtAsync"/>/<see cref="ReadFromAsync"/>
/// seek straight to the right spot instead of scanning from the start. This replica is the file's
/// sole writer, so positions can be computed synchronously ahead of each write and only committed
/// to the index once that write is confirmed durable — no reliance on read-back ordering.
/// </summary>
public sealed class WalRaftLog : IRaftLog, IAsyncDisposable {
    readonly WalWriter _walWriter;
    readonly WalReader _walReader;
    readonly Dictionary<ulong, long> _positionByIndex;
    long _nextWritePosition;
    ulong _lastIndex;
    ulong _lastTerm;

    WalRaftLog(WalWriter walWriter, WalReader walReader, Dictionary<ulong, long> positionByIndex, long nextWritePosition, ulong lastIndex, ulong lastTerm) {
        _walWriter = walWriter;
        _walReader = walReader;
        _positionByIndex = positionByIndex;
        _nextWritePosition = nextWritePosition;
        _lastIndex = lastIndex;
        _lastTerm = lastTerm;
    }

    /// <inheritdoc />
    public ulong LastIndex => _lastIndex;

    /// <inheritdoc />
    public ulong LastTerm => _lastTerm;

    // Compaction (CompactToAsync) is not implemented yet — there is no snapshot base below
    // which entries are ever discarded, so this is always zero for now.
    /// <inheritdoc />
    public ulong SnapshotIndex => 0;

    /// <inheritdoc />
    public ulong SnapshotTerm => 0;

    /// <summary>
    /// Opens (creating if necessary) the WAL file at <paramref name="path"/>, rebuilding the
    /// position index with a single sequential scan — the same torn-write-stops-cleanly ending
    /// as the storage layer's own recovery: whatever a scan can't fully read back was never durable.
    /// </summary>
    public static async Task<WalRaftLog> OpenAsync(string path, CancellationToken cancellationToken = default) {
        if (!File.Exists(path))
            File.Create(path).Dispose();

        var reader = new WalReader(path);
        var positionByIndex = new Dictionary<ulong, long>();
        long nextWritePosition = 0;
        ulong lastIndex = 0, lastTerm = 0;

        await foreach (var result in reader.ReadFromAsync(0, cancellationToken)) {
            using (result) {
                positionByIndex[result.Record.RaftIndex] = nextWritePosition;
                nextWritePosition += result.BytesConsumed;
                lastIndex = result.Record.RaftIndex;
                lastTerm = result.Record.RaftTerm;
            }
        }

        var writer = await WalWriter.OpenAsync(path, cancellationToken);
        return new WalRaftLog(writer, reader, positionByIndex, nextWritePosition, lastIndex, lastTerm);
    }

    /// <inheritdoc />
    public async ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken cancellationToken = default) {
        if (index == SnapshotIndex)
            return SnapshotTerm;

        await foreach (var record in ReadFromAsync(index, cancellationToken))
            return record.RaftTerm;

        throw new InvalidOperationException($"No entry at index {index}.");
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<WalRecord> ReadFromAsync(ulong fromIndex, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        if (fromIndex > _lastIndex)
            yield break;

        await foreach (var result in _walReader.ReadFromAsync(_positionByIndex[fromIndex], cancellationToken)) {
            using (result)
                yield return result.Record;
        }
    }

    /// <inheritdoc />
    public async ValueTask AppendAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default) {
        if (entries.Count == 0)
            return;

        // Positions are computed up front, against the current tail — nothing but this call
        // writes to this file, so the layout of this batch is fully known before any byte hits
        // disk. Held locally, not applied to `_positionByIndex`/`_nextWritePosition` until every
        // entry is confirmed durable below: a failed append must leave no trace in the index.
        var positions = new (ulong Index, long Position)[entries.Count];
        var position = _nextWritePosition;
        for (var i = 0; i < entries.Count; i++) {
            positions[i] = (entries[i].RaftIndex, position);
            position += WalRecordSerializer.GetEncodedLength(entries[i]);
        }

        var appends = new Task[entries.Count];
        for (var i = 0; i < entries.Count; i++)
            appends[i] = _walWriter.AppendAsync(entries[i], cancellationToken).AsTask();
        await Task.WhenAll(appends);

        foreach (var (index, entryPosition) in positions)
            _positionByIndex[index] = entryPosition;
        _nextWritePosition = position;

        var last = entries[^1];
        _lastIndex = last.RaftIndex;
        _lastTerm = last.RaftTerm;
    }

    /// <inheritdoc />
    public async ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken cancellationToken = default) {
        var truncateAt = _positionByIndex[fromIndex];
        await _walWriter.TruncateFromAsync(truncateAt, cancellationToken);

        for (var index = fromIndex; index <= _lastIndex; index++)
            _positionByIndex.Remove(index);

        _nextWritePosition = truncateAt;
        _lastIndex = fromIndex - 1;
        _lastTerm = _lastIndex == SnapshotIndex ? SnapshotTerm : await GetTermAtAsync(_lastIndex, cancellationToken);
    }

    // Snapshots are not implemented yet — see IRaftLog.CompactToAsync.
    /// <inheritdoc />
    public ValueTask CompactToAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException("Snapshot compaction is not implemented yet.");

    /// <summary>Disposes the underlying <see cref="WalWriter"/>, draining it to durable completion first.</summary>
    public ValueTask DisposeAsync() => _walWriter.DisposeAsync();
}
