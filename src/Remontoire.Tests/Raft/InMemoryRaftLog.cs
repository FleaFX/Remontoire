using System.Runtime.CompilerServices;
using Remontoire.Storage;

namespace Remontoire.Raft;

/// <summary>
/// An in-memory <see cref="IRaftLog"/> — no disk, no temp directories. Justifies
/// <see cref="IRaftLog"/>'s existence alongside <c>WalRaftLog</c>: the core state-machine tests
/// (election, replication, invariants) run fast and deterministically against this instead.
/// </summary>
sealed class InMemoryRaftLog : IRaftLog {
    readonly List<WalRecord> _entries = [];

    public ulong SnapshotIndex { get; private set; }
    public ulong SnapshotTerm { get; private set; }

    public ulong LastIndex => SnapshotIndex + (ulong)_entries.Count;
    public ulong LastTerm => _entries.Count > 0 ? _entries[^1].RaftTerm : SnapshotTerm;

    public ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(index == SnapshotIndex ? SnapshotTerm : _entries[PositionOf(index)].RaftTerm);

    public async IAsyncEnumerable<WalRecord> ReadFromAsync(ulong fromIndex, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        await Task.Yield();

        for (var position = Math.Max(PositionOf(fromIndex), 0); position < _entries.Count; position++) {
            cancellationToken.ThrowIfCancellationRequested();
            yield return _entries[position];
        }
    }

    // Copies each record's variable-length fields before storing — a real IRaftLog durably
    // rewrites the bytes into its own storage (a WAL file, decoupled from the caller's memory
    // the moment the write returns), so a caller is entitled to assume the same here. Without
    // this, a record decoded from the wire via WalRecordSerializer.TryRead (whose ReadOnlyMemory
    // fields are backed by a pooled buffer, returned to the pool as soon as the decoding method's
    // own using/finally block ends) would leave this log holding onto memory that gets silently
    // reused — and corrupted — by whatever the pool hands out next.
    public ValueTask AppendAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default) {
        foreach (var entry in entries)
            _entries.Add(entry with {
                PartitionKey = entry.PartitionKey.ToArray(),
                Headers = entry.Headers.Select(header => new Header(header.Key.ToArray(), header.Value.ToArray())).ToArray(),
                Payload = entry.Payload.ToArray(),
            });

        return ValueTask.CompletedTask;
    }

    public ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken cancellationToken = default) {
        var position = PositionOf(fromIndex);
        _entries.RemoveRange(position, _entries.Count - position);
        return ValueTask.CompletedTask;
    }

    public ValueTask CompactToAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken cancellationToken = default) {
        _entries.RemoveRange(0, (int)(lastIncludedIndex - SnapshotIndex));
        SnapshotIndex = lastIncludedIndex;
        SnapshotTerm = lastIncludedTerm;
        return ValueTask.CompletedTask;
    }

    public ValueTask InstallSnapshotAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken cancellationToken = default) {
        _entries.Clear();
        SnapshotIndex = lastIncludedIndex;
        SnapshotTerm = lastIncludedTerm;
        return ValueTask.CompletedTask;
    }

    int PositionOf(ulong index) => (int)(index - SnapshotIndex) - 1;
}
