using System.Buffers.Binary;
using Microsoft.Win32.SafeHandles;
using Remontoire.Storage.Serialization;

namespace Remontoire.Storage;

/// <summary>
/// Reads from an immutable SST segment written by <see cref="SstWriter"/>. Uses
/// <see cref="RandomAccess"/> (explicit offset per call, no shared cursor) rather than a
/// <see cref="FileStream"/>'s mutable <c>Position</c> — a segment is read by potentially many
/// concurrent callers at once (e.g. several subscriptions reading historical data), and a
/// shared, seekable stream would let one caller's seek corrupt another's in-flight read.
/// </summary>
sealed class SstSegment : IDisposable {
    readonly SafeFileHandle _handle;
    readonly long _dataEndPosition;
    readonly SstIndexEntry[] _index;

    public ulong MinOffset { get; }
    public ulong MaxOffset { get; }

    SstSegment(SafeFileHandle handle, ulong minOffset, ulong maxOffset, long dataEndPosition, SstIndexEntry[] index) {
        _handle = handle;
        MinOffset = minOffset;
        MaxOffset = maxOffset;
        _dataEndPosition = dataEndPosition;
        _index = index;
    }

    /// <summary>
    /// Opens the SST segment at <paramref name="path"/>, reading and validating its footer and
    /// loading its sparse index into memory.
    /// </summary>
    /// <exception cref="InvalidDataException">The file is not a valid, uncorrupted SST segment.</exception>
    public static async Task<SstSegment> OpenAsync(string path, CancellationToken cancellationToken = default) {
        var handle = File.OpenHandle(path);

        // Ownership of `handle` only transfers to the returned SstSegment on success — any
        // exception below (bad magic, invalid footer) must dispose it here, or the handle leaks.
        try {
            var fileLength = RandomAccess.GetLength(handle);
            var footer = new byte[Sst.FooterLength];
            await RandomAccess.ReadAsync(handle, footer, fileLength - Sst.FooterLength, cancellationToken);

            if (!footer.AsSpan(0, 8).SequenceEqual(Sst.Magic))
                throw new InvalidDataException($"'{path}' is not a valid SST segment (bad magic).");

            var minOffset = BinaryPrimitives.ReadUInt64LittleEndian(footer.AsSpan(8, 8));
            var maxOffset = BinaryPrimitives.ReadUInt64LittleEndian(footer.AsSpan(16, 8));
            var indexOffset = (long)BinaryPrimitives.ReadUInt64LittleEndian(footer.AsSpan(24, 8));
            var indexLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(footer.AsSpan(32, 4));

            var dataSize = fileLength - Sst.FooterLength;
            if (indexOffset < 0 || indexOffset > dataSize)
                throw new InvalidDataException($"'{path}' has an invalid index offset {indexOffset}.");
            if (indexLength < 0 || indexLength % 16 != 0 || indexOffset + indexLength > dataSize)
                throw new InvalidDataException($"'{path}' has an invalid index length {indexLength}.");
            if (minOffset > maxOffset)
                throw new InvalidDataException($"'{path}' has MinOffset {minOffset} greater than MaxOffset {maxOffset}.");

            var indexBytes = new byte[indexLength];
            await RandomAccess.ReadAsync(handle, indexBytes, indexOffset, cancellationToken);

            var index = new SstIndexEntry[indexLength / 16];
            for (var i = 0; i < index.Length; i++) {
                var offset = BinaryPrimitives.ReadUInt64LittleEndian(indexBytes.AsSpan(i * 16, 8));
                var position = (long)BinaryPrimitives.ReadUInt64LittleEndian(indexBytes.AsSpan(i * 16 + 8, 8));
                index[i] = new SstIndexEntry(offset, position);
            }

            return new SstSegment(handle, minOffset, maxOffset, indexOffset, index);
        } catch {
            handle.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The caller must dispose the returned <see cref="LogEntryReadResult"/> when
    /// <see langword="true"/> — it owns a pooled buffer.
    /// </summary>
    public bool TryGet(ulong logicalOffset, out LogEntryReadResult result) {
        result = default;
        if (logicalOffset < MinOffset || logicalOffset > MaxOffset)
            return false;

        // Offsets are gapless within a segment and ScanFrom already starts at-or-after
        // logicalOffset, so the very first result — given the range check above already
        // passed — is always the exact match. The disposal/false fallback below is defensive,
        // not expected to trigger in practice.
        foreach (var candidate in ScanFrom(logicalOffset)) {
            if (candidate.Entry.LogicalOffset == logicalOffset) {
                result = candidate;
                return true;
            }

            candidate.Dispose();
            break;
        }

        return false;
    }

    /// <summary>
    /// The caller must dispose each yielded <see cref="LogEntryReadResult"/> — it owns a
    /// pooled buffer.
    /// </summary>
    public IEnumerable<LogEntryReadResult> ScanFrom(ulong logicalOffset) =>
        EnumerateFrom(LocateStartPosition(logicalOffset), logicalOffset);

    long LocateStartPosition(ulong logicalOffset) {
        var lo = 0;
        var hi = _index.Length - 1;
        var found = 0;

        while (lo <= hi) {
            var mid = (lo + hi) / 2;
            if (_index[mid].Offset <= logicalOffset) { found = mid; lo = mid + 1; }
            else hi = mid - 1;
        }

        return _index[found].Position;
    }

    IEnumerable<LogEntryReadResult> EnumerateFrom(long position, ulong logicalOffset) {
        while (position < _dataEndPosition) {
            var result = ReadRecordAt(ref position);
            if (result.Status != LogEntryReadStatus.Success) {
                result.Dispose();
                yield break; // a segment is immutable and only ever written by SstWriter — should not happen
            }

            if (result.Entry.LogicalOffset < logicalOffset) {
                result.Dispose();
                continue;
            }

            yield return result;
        }
    }

    // Two reads per record (a small header peek, then the full record) — simpler than
    // speculative read-ahead, and still page-cache-backed for an immutable, recently-written
    // segment. Not a phase-1 bottleneck; a read-ahead optimization is possible later without
    // changing this method's contract.
    LogEntryReadResult ReadRecordAt(ref long position) {
        Span<byte> header = stackalloc byte[8]; // RecordLength(4) + CRC32C(4)
        RandomAccess.Read(_handle, header, position);

        var recordLength = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(0, 4));
        var totalLength = 8 + (int)recordLength;

        var buffer = new byte[totalLength];
        RandomAccess.Read(_handle, buffer, position);
        position += totalLength;

        return LogEntrySerializer.TryRead(buffer);
    }

    /// <inheritdoc />
    public void Dispose() =>
        _handle.Dispose();
}
