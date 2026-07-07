namespace Remontoire.Storage;

/// <summary>
/// The in-memory, most-recently-written slice of a shard's log — everything appended since
/// the last SST flush. Single-writer (only <c>Remontoire.Storage.Serialization.WalRecordApplier</c>
/// calls <see cref="Append"/>); safe for concurrent, lock-free reads while the writer is active.
/// </summary>
sealed class MemTable : IDisposable {
    const int BlockCapacity = 1024;
    readonly SingleWriterGuard _writerGuard = new();

    ulong _firstOffset;
    Block[] _blocks = [];
    long _estimatedSizeBytes;

    /// <summary>
    /// An estimate, in bytes, of this MemTable's current size — used to decide when to flush
    /// to an SST segment. Not exact (ignores per-entry/array overhead), good enough for a
    /// threshold check.
    /// </summary>
    public long EstimatedSizeBytes => Volatile.Read(ref _estimatedSizeBytes);

    /// <summary>
    /// Appends <paramref name="entry"/>. Its <c>LogicalOffset</c> must be exactly one greater
    /// than the previously appended entry's (or, for the first entry, whatever offset this
    /// MemTable starts at) — offsets are assumed gapless and strictly increasing. Only ever
    /// meant to be called by one single-threaded driver at a time — see
    /// <see cref="SingleWriterGuard"/>, which the wait-free reads elsewhere in this class
    /// depend on.
    /// </summary>
    public void Append(LogEntry entry) {
        using var _ = _writerGuard.Enter();

        var blocks = _blocks;

        if (blocks.Length == 0)
            _firstOffset = entry.LogicalOffset;

        var (blockIndex, withinBlock) = Locate(entry.LogicalOffset, _firstOffset);

        if (blockIndex >= blocks.Length) {
            var grown = new Block[blockIndex + 1];
            Array.Copy(blocks, grown, blocks.Length);
            for (var i = blocks.Length; i < grown.Length; i++)
                grown[i] = new Block(BlockCapacity);

            Volatile.Write(ref _blocks, grown); // safe publication — see Block.Add's remarks
            blocks = grown;
        }

        var stored = blocks[blockIndex].Add(withinBlock, entry);
        Volatile.Write(ref _estimatedSizeBytes, _estimatedSizeBytes + EstimateSize(stored));
    }

    /// <summary>
    /// Attempts to read the entry at <paramref name="logicalOffset"/>.
    /// </summary>
    public bool TryGet(ulong logicalOffset, out LogEntry entry) {
        entry = default;
        var blocks = Volatile.Read(ref _blocks);

        if (blocks.Length == 0 || logicalOffset < _firstOffset)
            return false;

        var (blockIndex, withinBlock) = Locate(logicalOffset, _firstOffset);

        if (blockIndex >= blocks.Length || withinBlock >= blocks[blockIndex].Count)
            return false;

        entry = blocks[blockIndex].Get(withinBlock);
        return true;
    }

    /// <summary>
    /// Enumerates entries from <paramref name="logicalOffset"/> onward, as of the moment this
    /// method is called — entries appended after this call are not included, even if the
    /// caller has not started (or is still) enumerating when they arrive.
    /// </summary>
    public IEnumerable<LogEntry> ScanFrom(ulong logicalOffset) {
        // Snapshot eagerly, right here — NOT inside EnumerateFrom below. ScanFrom itself must
        // stay an ordinary (non-iterator) method for that: a `yield`-based method only runs its
        // body up to the first `yield` on the caller's *first* MoveNext(), not on this call, which
        // would silently move the snapshot to whenever enumeration happens to start instead of
        // to this call, as documented.
        var blocks = Volatile.Read(ref _blocks);
        if (blocks.Length == 0)
            return [];

        var (blockIndex, withinBlock) = Locate(Math.Max(logicalOffset, _firstOffset), _firstOffset);
        if (blockIndex >= blocks.Length)
            return [];

        // Every block except the last is already permanently full by construction — Append
        // only ever creates a new block once the previous one has reached exactly BlockCapacity
        // entries. So the *only* value that can still change after this call is the last
        // block's count; freezing that one value is enough for a fully consistent snapshot,
        // without needing to freeze anything for the (already-immutable) earlier blocks.
        var lastBlockFrozenCount = blocks[^1].Count;
        return EnumerateFrom(blocks, blockIndex, withinBlock, lastBlockFrozenCount);
    }

    static IEnumerable<LogEntry> EnumerateFrom(Block[] blocks, int blockIndex, int withinBlock, int lastBlockFrozenCount) {
        for (; blockIndex < blocks.Length; blockIndex++) {
            var count = blockIndex == blocks.Length - 1 ? lastBlockFrozenCount : BlockCapacity;
            for (; withinBlock < count; withinBlock++)
                yield return blocks[blockIndex].Get(withinBlock);
            withinBlock = 0;
        }
    }

    public void Dispose() {
        foreach (var block in Volatile.Read(ref _blocks))
            block.Dispose();
    }

    static long EstimateSize(in LogEntry entry) {
        var size = 48L + entry.PartitionKey.Length + entry.Payload.Length; // 48 = rough fixed overhead
        foreach (var header in entry.Headers)
            size += header.Key.Length + header.Value.Length;
        return size;
    }

    // Assumes logicalOffset >= firstOffset — every call site above guarantees this itself
    // (Append: firstOffset is set from the very first entry; TryGet: already checked;
    // ScanFrom: clamped via Math.Max), so this stays a simple, unconditional calculation.
    static (int BlockIndex, int IndexWithinBlock) Locate(ulong logicalOffset, ulong firstOffset) {
        var index = logicalOffset - firstOffset;
        return ((int)(index / BlockCapacity), (int)(index % BlockCapacity));
    }
}
