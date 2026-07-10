using System.Runtime.CompilerServices;

namespace Remontoire.Storage;

public sealed partial class ShardLog {
    /// <summary>
    /// Attempts to read the entry at <paramref name="logicalOffset"/>. The caller must dispose
    /// the returned <see cref="LogEntryHandle"/> when this returns <see langword="true"/>.
    /// </summary>
    public bool TryGet(ulong logicalOffset, out LogEntryHandle handle) {
        if (Volatile.Read(ref _memTable).TryGet(logicalOffset, out var entry)) {
            handle = new LogEntryHandle(entry);
            return true;
        }

        foreach (var segment in Volatile.Read(ref _segments)) {
            if (logicalOffset < segment.MinOffset || logicalOffset > segment.MaxOffset)
                continue;

            if (segment.TryGet(logicalOffset, out var result)) {
                handle = result.ToHandle(); // NEVER wrap `result` in `using` here — see ToHandle's contract
                return true;
            }
        }

        handle = default;
        return false;
    }

    /// <summary>
    /// Enumerates entries from <paramref name="logicalOffset"/> onward, as of the moment this
    /// method is called — segments first (oldest to newest), then whatever was in the MemTable
    /// at that moment. The caller must dispose each yielded <see cref="LogEntryHandle"/>.
    /// </summary>
    public IAsyncEnumerable<LogEntryHandle> ReadFromAsync(ulong logicalOffset, CancellationToken cancellationToken = default) {
        // Snapshot eagerly, right here — NOT inside EnumerateFromAsync below, and in the same
        // order TryGet reads these fields (MemTable first, then segments). ReadFromAsync itself
        // must stay an ordinary (non-iterator) method for both reasons: an iterator method would
        // defer these reads to the caller's first MoveNextAsync (same pitfall MemTable.ScanFrom's
        // own remarks describe), and reading segments before MemTable — the reverse of TryGet's
        // safe order — can observe neither the entry's old MemTable (already emptied by a
        // concurrent flush) nor its new segment (published before this call started reading),
        // skipping it entirely for this call.
        var memTable = Volatile.Read(ref _memTable);
        var segments = Volatile.Read(ref _segments);
        return EnumerateFromAsync(segments, memTable, logicalOffset, cancellationToken);
    }

    static async IAsyncEnumerable<LogEntryHandle> EnumerateFromAsync(
        SstSegment[] segments, MemTable memTable, ulong logicalOffset, [EnumeratorCancellation] CancellationToken cancellationToken) {
        foreach (var segment in segments) {
            cancellationToken.ThrowIfCancellationRequested();
            if (segment.MaxOffset < logicalOffset)
                continue;

            foreach (var result in segment.ScanFrom(Math.Max(logicalOffset, segment.MinOffset)))
                yield return result.ToHandle(); // NEVER `using (result)` here — see TryGet's note
        }

        foreach (var entry in memTable.ScanFrom(logicalOffset))
            yield return new LogEntryHandle(entry);
    }

    /// <summary>
    /// Republishes every record this shard log receives from its committed-source — <see
    /// cref="WalRecordType.Append"/> records after materializing them into MemTable/SST,
    /// everything else (<see cref="WalRecordType.Ack"/>, <see cref="WalRecordType.ShardConfigChange"/>,
    /// a NoOp) untouched and unmaterialized. The single feed for anything downstream of storage
    /// that needs the raw committed stream without a second subscription to the committed-source's
    /// own single reader slot. Only one consumer may enumerate this at a time — same discipline
    /// as the committed-source itself.
    /// </summary>
    public IAsyncEnumerable<WalRecord> ReadAppliedAsync(CancellationToken cancellationToken = default) =>
        _applied.Reader.ReadAllAsync(cancellationToken);

    /// <summary>
    /// Completes the next time this shard log applies an <see cref="WalRecordType.Append"/>
    /// record — a signal, not a data channel, so any number of concurrent waiters may await it
    /// independently. The caller re-reads (<see cref="TryGet"/>/<see cref="ReadFromAsync"/>) after
    /// it completes; this never carries the record itself, only "something changed, look again."
    /// </summary>
    public Task WaitForAppendAsync(CancellationToken cancellationToken = default) => _appended.Task.WaitAsync(cancellationToken);
}
