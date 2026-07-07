using System.Buffers;
using System.Buffers.Binary;

namespace Remontoire.Storage.Serialization;

/// <summary>
/// Converts <see cref="WalRecord"/>s to and from their on-disk binary representation.
/// </summary>
static class WalRecordSerializer {
    const byte CurrentVersion = 0x01;
    const int PrefixLength = 5; // Version(1) + RecordLength(4)
    const int HeaderLengthPrefixesOverhead = 2 + 4; // per header: key-length(2) + value-length(4)

    /// <summary>
    /// Computes the total on-disk size, in bytes, of <paramref name="record"/>.
    /// </summary>
    public static int GetEncodedLength(in WalRecord record) => PrefixLength + 4 + 1 + 8 + 8 + 8 + 8 + 2 + record.PartitionKey.Length + 2 + record.Headers.Sum(header => 2 + header.Key.Length + 4 + header.Value.Length) + 4 + record.Payload.Length;

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
        writer.WriteBytesWithUInt16Length(record.PartitionKey.Span);

        writer.WriteUInt16((ushort)record.Headers.Count);
        foreach (var header in record.Headers) {
            writer.WriteBytesWithUInt16Length(header.Key.Span);
            writer.WriteBytesWithUInt32Length(header.Value.Span);
        }

        writer.WriteBytesWithUInt32Length(record.Payload.Span);
    }

    static bool TryParseBody(ReadOnlySpan<byte> body, out WalRecord record, out IMemoryOwner<byte>? owner) {
        record = default;
        owner = null;
        var reader = new SpanReader(body);

        if (!reader.TryReadByte(out var recordTypeByte)) return false;
        if (!reader.TryReadUInt64(out var raftTerm)) return false;
        if (!reader.TryReadUInt64(out var raftIndex)) return false;
        if (!reader.TryReadUInt64(out var logicalOffset)) return false;
        if (!reader.TryReadUInt64(out var timestampMicros)) return false;
        if (!reader.TryReadBytesWithUInt16Length(out var partitionKeySpan)) return false;

        if (!reader.TryReadUInt16(out var headerCount)) return false;

        // Everything left in `body` is exactly headerCount * (2 + keyLen + 4 + valueLen) +
        // 4 (PayloadLength) + payloadLen. Each header's length-prefixes contribute a fixed,
        // known overhead regardless of key/value content, so the total variable-length
        // payload size (partition key + every header's key/value + payload) is computable
        // right now, without a separate pre-pass over the headers.
        var variableLength = partitionKeySpan.Length + reader.Remaining - HeaderLengthPrefixesOverhead * headerCount - 4;
        if (variableLength < 0)
            return false;

        owner = MemoryPool<byte>.Shared.Rent(variableLength);
        var destination = owner.Memory;
        var position = 0;

        var partitionKey = CopyInto(partitionKeySpan, destination, ref position);

        var headers = headerCount == 0 ? [] : new WalHeader[headerCount];
        for (var i = 0; i < headerCount; i++) {
            if (!reader.TryReadBytesWithUInt16Length(out var keySpan) || !reader.TryReadBytesWithUInt32Length(out var valueSpan)) {
                owner.Dispose();
                owner = null;
                return false;
            }

            headers[i] = new WalHeader(CopyInto(keySpan, destination, ref position), CopyInto(valueSpan, destination, ref position));
        }

        if (!reader.TryReadBytesWithUInt32Length(out var payloadSpan)) {
            owner.Dispose();
            owner = null;
            return false;
        }

        var payload = CopyInto(payloadSpan, destination, ref position);

        record = new WalRecord((WalRecordType)recordTypeByte, raftTerm, raftIndex, logicalOffset, timestampMicros, partitionKey, headers, payload);
        return true;
    }

    static ReadOnlyMemory<byte> CopyInto(ReadOnlySpan<byte> source, Memory<byte> destination, ref int position) {
        source.CopyTo(destination.Span.Slice(position, source.Length));
        var written = destination.Slice(position, source.Length);
        position += source.Length;
        return written;
    }
}
