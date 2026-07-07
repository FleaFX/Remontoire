using System.Buffers;
using System.Buffers.Binary;

namespace Remontoire.Storage.Serialization;

enum LogEntryReadStatus {
    Success,
    Incomplete,
    Corrupt,
}

/// <summary>
/// The result of <see cref="LogEntrySerializer.TryRead"/>. Disposing releases the pooled
/// buffer backing <see cref="Entry"/>'s variable-length fields, if any — safe regardless of
/// <see cref="Status"/>.
/// </summary>
readonly struct LogEntryReadResult(LogEntryReadStatus status, LogEntry entry, int bytesConsumed, IMemoryOwner<byte>? owner) : IDisposable {
    public LogEntryReadStatus Status { get; } = status;
    public LogEntry Entry { get; } = entry;
    public int BytesConsumed { get; } = bytesConsumed;

    /// <summary>
    /// Converts to a public <see cref="LogEntryHandle"/>, handing off ownership of the pooled
    /// buffer — call exactly once per result, right before this struct's value is discarded.
    /// </summary>
    internal LogEntryHandle ToHandle() => new(Entry, owner);

    public void Dispose() => owner?.Dispose();
}

/// <summary>
/// Converts <see cref="LogEntry"/>s to and from their on-disk binary representation — the
/// per-record format used inside an SST segment.
/// </summary>
static class LogEntrySerializer {
    const int PrefixLength = 4 + 4; // RecordLength(4) + CRC32C(4)

    /// <summary>
    /// Computes the total on-disk size, in bytes, of <paramref name="entry"/>.
    /// </summary>
    public static int GetEncodedLength(in LogEntry entry) =>
        PrefixLength + 8 + 8 + VariableLengthTail.GetEncodedLength(entry.PartitionKey, entry.Headers, entry.Payload);

    /// <summary>
    /// Writes <paramref name="entry"/> to <paramref name="destination"/> and returns the
    /// number of bytes written. <paramref name="destination"/> must be at least
    /// <see cref="GetEncodedLength"/> bytes long.
    /// </summary>
    public static int Write(in LogEntry entry, Span<byte> destination) {
        var totalLength = GetEncodedLength(entry);
        if (destination.Length < totalLength)
            throw new ArgumentException("Destination buffer too small.", nameof(destination));

        var body = destination.Slice(PrefixLength, totalLength - PrefixLength);
        WriteBody(entry, body);

        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), (uint)(totalLength - PrefixLength));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(4, 4), Crc32C.ComputeHash(body));

        return totalLength;
    }

    /// <summary>
    /// Attempts to read one record from the start of <paramref name="source"/>. The caller
    /// must dispose the returned <see cref="LogEntryReadResult"/> once done with its
    /// <see cref="LogEntryReadResult.Entry"/> — it owns a pooled buffer.
    /// </summary>
    public static LogEntryReadResult TryRead(ReadOnlySpan<byte> source) {
        if (source.Length < PrefixLength)
            return new LogEntryReadResult(LogEntryReadStatus.Incomplete, default, 0, null);

        var recordLength = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(0, 4));
        var totalLength = PrefixLength + (int)recordLength;

        if (source.Length < totalLength)
            return new LogEntryReadResult(LogEntryReadStatus.Incomplete, default, 0, null);

        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(4, 4));
        var body = source.Slice(PrefixLength, totalLength - PrefixLength);

        if (Crc32C.ComputeHash(body) != expectedCrc)
            return new LogEntryReadResult(LogEntryReadStatus.Corrupt, default, 0, null);

        if (!TryParseBody(body, out var entry, out var owner))
            return new LogEntryReadResult(LogEntryReadStatus.Corrupt, default, 0, null);

        return new LogEntryReadResult(LogEntryReadStatus.Success, entry, totalLength, owner);
    }

    static void WriteBody(in LogEntry entry, Span<byte> body) {
        var writer = new SpanWriter(body);
        writer.WriteUInt64(entry.LogicalOffset);
        writer.WriteUInt64(entry.TimestampMicros);
        VariableLengthTail.Write(ref writer, entry.PartitionKey.Span, entry.Headers, entry.Payload.Span);
    }

    static bool TryParseBody(ReadOnlySpan<byte> body, out LogEntry entry, out IMemoryOwner<byte>? owner) {
        entry = default;
        var reader = new SpanReader(body);

        if (!reader.TryReadUInt64(out var logicalOffset)) { owner = null; return false; }
        if (!reader.TryReadUInt64(out var timestampMicros)) { owner = null; return false; }

        if (!VariableLengthTail.TryParse(ref reader, out var partitionKey, out var headers, out var payload, out owner))
            return false;

        entry = new LogEntry(logicalOffset, timestampMicros, partitionKey, headers, payload);
        return true;
    }
}
