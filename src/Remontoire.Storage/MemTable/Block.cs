using System.Buffers;

namespace Remontoire.Storage;

/// <summary>
/// A fixed-capacity, append-only slice of a <see cref="MemTable"/>. Copies each entry's
/// variable-length data into its own exactly-sized pooled rental — same technique as
/// <see cref="Serialization.WalRecordSerializer"/>'s record-parsing, applied here so a
/// <see cref="LogEntry"/> can safely outlive whatever buffer it originally arrived in
/// (typically a short-lived <see cref="Serialization.WalReadResult"/>).
/// </summary>
sealed class Block(int capacity) : IDisposable {
    readonly LogEntry[] _entries = new LogEntry[capacity];
    readonly IMemoryOwner<byte>?[] _owners = new IMemoryOwner<byte>?[capacity];
    int _count;

    /// <summary>
    /// The number of entries written so far. Readers must check this (and only read indices
    /// below it) before indexing into this block — see <see cref="Add"/> for why.
    /// </summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>
    /// Reads the entry at <paramref name="index"/>. The caller must have already confirmed
    /// <paramref name="index"/> is below <see cref="Count"/>.
    /// </summary>
    public LogEntry Get(int index) => _entries[index];

    /// <summary>
    /// Copies <paramref name="source"/>'s variable-length fields into a freshly rented,
    /// exactly-sized buffer, stores the result at <paramref name="index"/>, and returns it.
    /// Only ever called by a single writer, one index at a time, in increasing order.
    /// </summary>
    public LogEntry Add(int index, LogEntry source) {
        var variableLength = source.PartitionKey.Length + source.Payload.Length + source.Headers.Sum(header => header.Key.Length + header.Value.Length);

        var owner = MemoryPool<byte>.Shared.Rent(variableLength);
        var destination = owner.Memory;
        var position = 0;

        var partitionKey = CopyInto(source.PartitionKey.Span, destination, ref position);

        var headers = source.Headers.Count == 0 ? [] : new WalHeader[source.Headers.Count];
        for (var i = 0; i < source.Headers.Count; i++) {
            var key = CopyInto(source.Headers[i].Key.Span, destination, ref position);
            var value = CopyInto(source.Headers[i].Value.Span, destination, ref position);
            headers[i] = new WalHeader(key, value);
        }

        var payload = CopyInto(source.Payload.Span, destination, ref position);
        var stored = new LogEntry(source.LogicalOffset, source.TimestampMicros, partitionKey, headers, payload);

        _entries[index] = stored;
        _owners[index] = owner;

        // Publish only after the entry and its owner are both fully written — a reader that
        // observes Count > index via Volatile.Read is then guaranteed to see this entry
        // exactly as written here, never a torn mix of old/new fields (LogEntry is larger than
        // a pointer, so the plain array assignment above is not itself atomic).
        Volatile.Write(ref _count, index + 1);

        return stored;
    }

    static ReadOnlyMemory<byte> CopyInto(ReadOnlySpan<byte> source, Memory<byte> destination, ref int position) {
        source.CopyTo(destination.Span.Slice(position, source.Length));
        var written = destination.Slice(position, source.Length);
        position += source.Length;
        return written;
    }

    public void Dispose() {
        foreach (var owner in _owners)
            owner?.Dispose();
    }
}
