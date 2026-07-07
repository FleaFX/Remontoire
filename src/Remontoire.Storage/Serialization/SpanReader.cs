using System.Buffers.Binary;

namespace Remontoire.Storage.Serialization;

/// <summary>
/// Reads primitive values sequentially from a <see cref="ReadOnlySpan{T}"/>, tracking
/// position internally. Every <c>TryRead*</c> method returns <see langword="false"/> — never
/// throws — when fewer bytes remain than the value needs, so callers can treat "not enough
/// data yet" as an ordinary, expected outcome rather than an exception.
/// </summary>
ref struct SpanReader(ReadOnlySpan<byte> source) {
    readonly ReadOnlySpan<byte> _source = source;
    int _position;

    /// <summary>
    /// The number of bytes not yet consumed.
    /// </summary>
    internal int Remaining => _source.Length - _position;

    /// <summary>
    /// Attempts to read a single byte.
    /// </summary>
    public bool TryReadByte(out byte value) {
        if (Remaining < 1) { value = default; return false; }
        value = _source[_position];
        _position += 1;
        return true;
    }

    /// <summary>
    /// Attempts to read a little-endian <see cref="ushort"/>.
    /// </summary>
    public bool TryReadUInt16(out ushort value) {
        if (Remaining < 2) { value = default; return false; }
        value = BinaryPrimitives.ReadUInt16LittleEndian(_source.Slice(_position, 2));
        _position += 2;
        return true;
    }

    /// <summary>
    /// Attempts to read a little-endian <see cref="uint"/>.
    /// </summary>
    public bool TryReadUInt32(out uint value) {
        if (Remaining < 4) { value = default; return false; }
        value = BinaryPrimitives.ReadUInt32LittleEndian(_source.Slice(_position, 4));
        _position += 4;
        return true;
    }

    /// <summary>
    /// Attempts to read a little-endian <see cref="ulong"/>.
    /// </summary>
    public bool TryReadUInt64(out ulong value) {
        if (Remaining < 8) { value = default; return false; }
        value = BinaryPrimitives.ReadUInt64LittleEndian(_source.Slice(_position, 8));
        _position += 8;
        return true;
    }

    /// <summary>
    /// Attempts to read a 16-bit length prefix followed by that many bytes.
    /// </summary>
    public bool TryReadBytesWithUInt16Length(out ReadOnlySpan<byte> value) {
        if (!TryReadUInt16(out var length) || Remaining < length) { value = default; return false; }
        value = _source.Slice(_position, length);
        _position += length;
        return true;
    }

    /// <summary>
    /// Attempts to read a 32-bit length prefix followed by that many bytes.
    /// </summary>
    public bool TryReadBytesWithUInt32Length(out ReadOnlySpan<byte> value) {
        if (!TryReadUInt32(out var length) || Remaining < length) { value = default; return false; }
        value = _source.Slice(_position, (int)length);
        _position += (int)length;
        return true;
    }
}
