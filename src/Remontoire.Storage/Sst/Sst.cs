namespace Remontoire.Storage;

// SST file format:
//   Data section  — sequential, LogEntrySerializer-encoded records
//   Index section — sparse index, each pair: LogicalOffset(8) + BytePosition(8)
//   Footer        — magic(8) + minOffset(8) + maxOffset(8) + indexOffset(8) + indexLength(4) = 36 bytes
static class Sst {
    internal const int FooterLength = 36;
    internal const int DefaultSparseInterval = 128;
    internal static ReadOnlySpan<byte> Magic => "RMTRSST\0"u8;
}

/// <summary>
/// One sparse-index entry: the byte position of the record at <see cref="Offset"/>.
/// </summary>
readonly record struct SstIndexEntry(ulong Offset, long Position);
