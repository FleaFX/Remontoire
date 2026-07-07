using System.Buffers;

namespace Remontoire.Storage.Serialization;

/// <summary>
/// The <c>PartitionKey</c>/<c>Headers</c>/<c>Payload</c> tail shared by every on-disk record
/// format in this project (<see cref="WalRecordSerializer"/>, <see cref="LogEntrySerializer"/>)
/// — everything after each format's own fixed-size header fields.
/// </summary>
static class VariableLengthTail {
    const int HeaderLengthPrefixesOverhead = 2 + 4; // per header: key-length(2) + value-length(4)

    /// <summary>
    /// Computes the on-disk size, in bytes, of the tail for the given
    /// <paramref name="partitionKey"/>/<paramref name="headers"/>/<paramref name="payload"/>.
    /// </summary>
    public static int GetEncodedLength(ReadOnlyMemory<byte> partitionKey, IReadOnlyList<WalHeader> headers, ReadOnlyMemory<byte> payload) =>
        2 + partitionKey.Length + 2 + headers.Sum(header => 2 + header.Key.Length + 4 + header.Value.Length) + 4 + payload.Length;

    /// <summary>
    /// Writes the tail to <paramref name="writer"/>'s current position onward.
    /// </summary>
    public static void Write(ref SpanWriter writer, ReadOnlySpan<byte> partitionKey, IReadOnlyList<WalHeader> headers, ReadOnlySpan<byte> payload) {
        writer.WriteBytesWithUInt16Length(partitionKey);

        writer.WriteUInt16((ushort)headers.Count);
        foreach (var header in headers) {
            writer.WriteBytesWithUInt16Length(header.Key.Span);
            writer.WriteBytesWithUInt32Length(header.Value.Span);
        }

        writer.WriteBytesWithUInt32Length(payload);
    }

    /// <summary>
    /// Parses the tail from <paramref name="reader"/>'s current position onward, renting
    /// exactly one <see cref="MemoryPool{T}"/> buffer sized to fit everything variable-length —
    /// same technique <see cref="WalRecordSerializer"/> originally introduced.
    /// </summary>
    public static bool TryParse(ref SpanReader reader, out ReadOnlyMemory<byte> partitionKey, out WalHeader[] headers, out ReadOnlyMemory<byte> payload, out IMemoryOwner<byte>? owner) {
        partitionKey = default;
        headers = [];
        payload = default;
        owner = null;

        if (!reader.TryReadBytesWithUInt16Length(out var partitionKeySpan)) return false;
        if (!reader.TryReadUInt16(out var headerCount)) return false;

        var variableLength = partitionKeySpan.Length + reader.Remaining - HeaderLengthPrefixesOverhead * headerCount - 4;
        if (variableLength < 0)
            return false;

        owner = MemoryPool<byte>.Shared.Rent(variableLength);
        var destination = owner.Memory;
        var position = 0;

        partitionKey = CopyInto(partitionKeySpan, destination, ref position);

        headers = headerCount == 0 ? [] : new WalHeader[headerCount];
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

        payload = CopyInto(payloadSpan, destination, ref position);
        return true;
    }

    static ReadOnlyMemory<byte> CopyInto(ReadOnlySpan<byte> source, Memory<byte> destination, ref int position) {
        source.CopyTo(destination.Span.Slice(position, source.Length));
        var written = destination.Slice(position, source.Length);
        position += source.Length;
        return written;
    }
}
