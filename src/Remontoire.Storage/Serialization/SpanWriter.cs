using System.Buffers.Binary;

namespace Remontoire.Storage.Serialization;

/// <summary>
/// Writes primitive values sequentially into a <see cref="Span{T}"/>, tracking position
/// internally so callers never do their own offset arithmetic.
/// </summary>
ref struct SpanWriter(Span<byte> destination) {
    readonly Span<byte> _destination = destination;
    int _position;

    /// <summary>
    /// Writes a single byte.
    /// </summary>
    public void WriteByte(byte value) {
        _destination[_position] = value;
        _position += 1;
    }

    /// <summary>
    /// Writes a little-endian <see cref="ushort"/>.
    /// </summary>
    public void WriteUInt16(ushort value) {
        BinaryPrimitives.WriteUInt16LittleEndian(_destination.Slice(_position, 2), value);
        _position += 2;
    }

    /// <summary>
    /// Writes a little-endian <see cref="uint"/>.
    /// </summary>
    public void WriteUInt32(uint value) {
        BinaryPrimitives.WriteUInt32LittleEndian(_destination.Slice(_position, 4), value);
        _position += 4;
    }

    /// <summary>
    /// Writes a little-endian <see cref="ulong"/>.
    /// </summary>
    public void WriteUInt64(ulong value) {
        BinaryPrimitives.WriteUInt64LittleEndian(_destination.Slice(_position, 8), value);
        _position += 8;
    }

    /// <summary>
    /// Writes a 16-bit length prefix followed by <paramref name="value"/>.
    /// </summary>
    public void WriteBytesWithUInt16Length(ReadOnlySpan<byte> value) {
        WriteUInt16((ushort)value.Length);
        value.CopyTo(_destination.Slice(_position, value.Length));
        _position += value.Length;
    }

    /// <summary>
    /// Writes a 32-bit length prefix followed by <paramref name="value"/>.
    /// </summary>
    public void WriteBytesWithUInt32Length(ReadOnlySpan<byte> value) {
        WriteUInt32((uint)value.Length);
        value.CopyTo(_destination.Slice(_position, value.Length));
        _position += value.Length;
    }
}
