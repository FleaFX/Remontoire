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
    /// Enumerates entries from <paramref name="logicalOffset"/> onward — segments first (oldest
    /// to newest), then whatever is still in the MemTable. The caller must dispose each yielded
    /// <see cref="LogEntryHandle"/>.
    /// </summary>
    public async IAsyncEnumerable<LogEntryHandle> ReadFromAsync(ulong logicalOffset, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        foreach (var segment in Volatile.Read(ref _segments)) {
            cancellationToken.ThrowIfCancellationRequested();
            if (segment.MaxOffset < logicalOffset)
                continue;

            foreach (var result in segment.ScanFrom(Math.Max(logicalOffset, segment.MinOffset)))
                yield return result.ToHandle(); // NEVER `using (result)` here — see TryGet's note
        }

        foreach (var entry in Volatile.Read(ref _memTable).ScanFrom(logicalOffset))
            yield return new LogEntryHandle(entry);
    }
}
