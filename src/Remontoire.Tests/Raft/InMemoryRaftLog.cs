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

    public ValueTask AppendAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default) {
        _entries.AddRange(entries);
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

    int PositionOf(ulong index) => (int)(index - SnapshotIndex) - 1;
}
