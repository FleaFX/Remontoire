using System.Buffers.Binary;
using System.Runtime.Intrinsics.X86;

namespace Remontoire.Storage.Serialization;

/// <summary>
/// Computes CRC-32C (Castagnoli) checksums. Deliberately not <c>System.IO.Hashing.Crc32</c>,
/// which implements the ISO-HDLC polynomial (zip/gzip/PNG), a different checksum entirely.
/// Hardware-accelerated via the x86 SSE4.2 CRC32 instruction, which computes Castagnoli
/// natively, with a table-driven software fallback for platforms without it.
/// </summary>
static class Crc32C {
    const uint InitialValue = 0xFFFFFFFF;
    static readonly uint[] SoftwareTable = BuildTable();

    /// <summary>
    /// Computes the CRC-32C checksum of <paramref name="data"/>.
    /// </summary>
    public static uint ComputeHash(ReadOnlySpan<byte> data) {
        if (Sse42.X64.IsSupported)
            return ComputeHardwareX64(data);

        if (Sse42.IsSupported)
            return ComputeHardware32(data);

        return ComputeSoftware(data);
    }

    /// <summary>
    /// Computes the checksum using the 64-bit x86 SSE4.2 hardware path. Exposed internally so
    /// tests can exercise this path directly, regardless of which path <see cref="ComputeHash"/>
    /// would pick on the machine actually running the tests.
    /// </summary>
    internal static uint ComputeHardwareX64(ReadOnlySpan<byte> data) {
        var crc = (ulong)InitialValue;
        var i = 0;

        while (data.Length - i >= 8) {
            var chunk = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(i, 8));
            crc = Sse42.X64.Crc32(crc, chunk);
            i += 8;
        }

        while (i < data.Length) {
            crc = Sse42.Crc32((uint)crc, data[i]);
            i++;
        }

        return (uint)crc ^ InitialValue;
    }

    /// <summary>
    /// Computes the checksum using the 32-bit x86 SSE4.2 hardware path. Exposed internally so
    /// tests can exercise this path directly, regardless of which path <see cref="ComputeHash"/>
    /// would pick on the machine actually running the tests.
    /// </summary>
    internal static uint ComputeHardware32(ReadOnlySpan<byte> data) {
        var crc = InitialValue;
        var i = 0;

        while (data.Length - i >= 4) {
            var chunk = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i, 4));
            crc = Sse42.Crc32(crc, chunk);
            i += 4;
        }

        while (i < data.Length) {
            crc = Sse42.Crc32(crc, data[i]);
            i++;
        }

        return crc ^ InitialValue;
    }

    /// <summary>
    /// Computes the checksum using the table-driven software fallback. Exposed internally so
    /// tests can exercise this path directly, regardless of which path <see cref="ComputeHash"/>
    /// would pick on the machine actually running the tests.
    /// </summary>
    internal static uint ComputeSoftware(ReadOnlySpan<byte> data) {
        var crc = InitialValue;

        foreach (var b in data)
            crc = SoftwareTable[(byte)(crc ^ b)] ^ (crc >> 8);

        return crc ^ InitialValue;
    }

    static uint[] BuildTable() {
        const uint polynomial = 0x82F63B78; // reversed Castagnoli polynomial
        var table = new uint[256];

        for (uint i = 0; i < 256; i++) {
            var c = i;
            for (var k = 0; k < 8; k++)
                c = (c & 1) != 0 ? polynomial ^ (c >> 1) : c >> 1;
            table[i] = c;
        }

        return table;
    }
}
