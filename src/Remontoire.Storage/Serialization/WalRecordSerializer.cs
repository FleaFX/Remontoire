using System.Buffers;
using System.Buffers.Binary;

namespace Remontoire.Storage.Serialization;

/// <summary>
/// Converts <see cref="WalRecord"/>s to and from their on-disk binary representation.
/// </summary>
static class WalRecordSerializer {
    const byte CurrentVersion = 0x01;
    const int PrefixLength = 5; // Version(1) + RecordLength(4)

    /// <summary>
    /// Computes the total on-disk size, in bytes, of <paramref name="record"/>.
    /// </summary>
    public static int GetEncodedLength(in WalRecord record) =>
        PrefixLength + 4 + 1 + 8 + 8 + 8 + 8 + VariableLengthTail.GetEncodedLength(record.PartitionKey, record.Headers, record.Payload);

    /// <summary>
    /// Writes <paramref name="record"/> to <paramref name="destination"/> and returns the
    /// number of bytes written. <paramref name="destination"/> must be at least
    /// <see cref="GetEncodedLength"/> bytes long.
    /// </summary>
    public static int Write(in WalRecord record, Span<byte> destination) {
        var totalLength = GetEncodedLength(record);
        if (destination.Length < totalLength)
            throw new ArgumentException("Destination buffer too small.", nameof(destination));

        var body = destination.Slice(PrefixLength + 4, totalLength - PrefixLength - 4);
        WriteBody(record, body);

        destination[0] = CurrentVersion;
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(1, 4), (uint)(totalLength - PrefixLength));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(5, 4), Crc32C.ComputeHash(body));

        return totalLength;
    }

    /// <summary>
    /// Attempts to read one record from the start of <paramref name="source"/>. The caller
    /// must dispose the returned <see cref="WalReadResult"/> once done with its
    /// <see cref="WalReadResult.Record"/> — it owns a pooled buffer.
    /// </summary>
    public static WalReadResult TryRead(ReadOnlySpan<byte> source) {
        if (source.Length < PrefixLength)
            return new WalReadResult(WalRecordReadStatus.Incomplete, default, 0, null);

        if (source[0] != CurrentVersion)
            return new WalReadResult(WalRecordReadStatus.Corrupt, default, 0, null);

        var recordLength = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(1, 4));
        var totalLength = PrefixLength + (int)recordLength;

        if (source.Length < totalLength)
            return new WalReadResult(WalRecordReadStatus.Incomplete, default, 0, null);

        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(5, 4));
        var body = source.Slice(PrefixLength + 4, totalLength - PrefixLength - 4);

        if (Crc32C.ComputeHash(body) != expectedCrc)
            return new WalReadResult(WalRecordReadStatus.Corrupt, default, 0, null);

        if (!TryParseBody(body, out var record, out var owner))
            return new WalReadResult(WalRecordReadStatus.Corrupt, default, 0, null);

        return new WalReadResult(WalRecordReadStatus.Success, record, totalLength, owner);
    }

    static void WriteBody(in WalRecord record, Span<byte> body) {
        var writer = new SpanWriter(body);

        writer.WriteByte((byte)record.RecordType);
        writer.WriteUInt64(record.RaftTerm);
        writer.WriteUInt64(record.RaftIndex);
        writer.WriteUInt64(record.LogicalOffset);
        writer.WriteUInt64(record.TimestampMicros);
        VariableLengthTail.Write(ref writer, record.PartitionKey.Span, record.Headers, record.Payload.Span);
    }

    static bool TryParseBody(ReadOnlySpan<byte> body, out WalRecord record, out IMemoryOwner<byte>? owner) {
        record = default;
        var reader = new SpanReader(body);

        if (!reader.TryReadByte(out var recordTypeByte)) { owner = null; return false; }
        if (!reader.TryReadUInt64(out var raftTerm)) { owner = null; return false; }
        if (!reader.TryReadUInt64(out var raftIndex)) { owner = null; return false; }
        if (!reader.TryReadUInt64(out var logicalOffset)) { owner = null; return false; }
        if (!reader.TryReadUInt64(out var timestampMicros)) { owner = null; return false; }

        if (!VariableLengthTail.TryParse(ref reader, out var partitionKey, out var headers, out var payload, out owner))
            return false;

        record = new WalRecord((WalRecordType)recordTypeByte, raftTerm, raftIndex, logicalOffset, timestampMicros, partitionKey, headers, payload);
        return true;
    }
}
