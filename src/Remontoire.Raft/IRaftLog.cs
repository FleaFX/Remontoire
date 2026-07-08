using Remontoire.Storage;

namespace Remontoire.Raft;

/// <summary>
/// The durable, ordered Raft log of one physical shard group. Indexes are one-based;
/// everything at or below <see cref="SnapshotIndex"/> has been compacted away.
/// Two implementations: <c>WalRaftLog</c> (the shard WAL, production) and
/// <c>InMemoryRaftLog</c> (deterministic core tests) — the latter justifies the interface.
/// </summary>
public interface IRaftLog {
    /// <summary>Index of the last entry, or <see cref="SnapshotIndex"/> when the tail is empty.</summary>
    ulong LastIndex { get; }

    /// <summary>Term of the entry at <see cref="LastIndex"/>, or <see cref="SnapshotTerm"/> when the tail is empty.</summary>
    ulong LastTerm { get; }

    /// <summary>Index of the last entry covered by the most recent snapshot; zero when none.</summary>
    ulong SnapshotIndex { get; }

    /// <summary>Term of the entry at <see cref="SnapshotIndex"/>; zero when none.</summary>
    ulong SnapshotTerm { get; }

    /// <summary>Returns the term of the entry at <paramref name="index"/> (which may equal <see cref="SnapshotIndex"/>).</summary>
    ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken cancellationToken = default);

    /// <summary>Reads entries from <paramref name="fromIndex"/> onward — used to catch up lagging followers.</summary>
    IAsyncEnumerable<WalRecord> ReadFromAsync(ulong fromIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Durably appends <paramref name="entries"/> — the returned task completes only after
    /// fsync (group-committed with concurrent appends, per <c>WalWriter</c>).
    /// </summary>
    ValueTask AppendAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Physically removes every entry at or after <paramref name="fromIndex"/> — the follower
    /// conflicting-suffix path. Only ever called with indexes above the commit index.
    /// </summary>
    ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances the log's base past <paramref name="lastIncludedIndex"/> after a snapshot:
    /// records the marker durably, then deletes WAL files entirely below it.
    /// </summary>
    ValueTask CompactToAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken cancellationToken = default);
}
