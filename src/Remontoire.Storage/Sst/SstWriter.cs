using System.Buffers;
using System.Buffers.Binary;
using Remontoire.Storage.Serialization;

namespace Remontoire.Storage;

/// <summary>
/// Writes a sorted sequence of <see cref="LogEntry"/>s to an immutable SST segment file.
/// </summary>
static class SstWriter {
    /// <summary>
    /// Writes <paramref name="sortedEntries"/> to <paramref name="path"/> as a new, immutable
    /// SST segment. Entries must already be sorted by <see cref="LogEntry.LogicalOffset"/>.
    /// </summary>
    public static async Task WriteAsync(string path, IEnumerable<LogEntry> sortedEntries, int indexIntervalRecords = Sst.DefaultSparseInterval, CancellationToken cancellationToken = default) {
        var tempPath = path + ".tmp";
        var index = new List<SstIndexEntry>();
        ulong minOffset = 0, maxOffset = 0;
        var recordCount = 0;
        long position = 0;

        await using (var file = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 0, useAsync: true)) {
            foreach (var entry in sortedEntries) {
                if (recordCount == 0)
                    minOffset = entry.LogicalOffset;
                maxOffset = entry.LogicalOffset;

                if (recordCount % indexIntervalRecords == 0)
                    index.Add(new SstIndexEntry(entry.LogicalOffset, position));

                var length = LogEntrySerializer.GetEncodedLength(entry);
                var buffer = ArrayPool<byte>.Shared.Rent(length);
                try {
                    var written = LogEntrySerializer.Write(entry, buffer);
                    await file.WriteAsync(buffer.AsMemory(0, written), cancellationToken);
                    position += written;
                } finally {
                    ArrayPool<byte>.Shared.Return(buffer);
                }

                recordCount++;
            }

            if (recordCount == 0)
                throw new ArgumentException("Cannot write an SST segment with no entries.", nameof(sortedEntries));

            var indexOffset = position;
            var indexBytes = new byte[index.Count * 16];
            for (var i = 0; i < index.Count; i++) {
                BinaryPrimitives.WriteUInt64LittleEndian(indexBytes.AsSpan(i * 16, 8), index[i].Offset);
                BinaryPrimitives.WriteUInt64LittleEndian(indexBytes.AsSpan(i * 16 + 8, 8), (ulong)index[i].Position);
            }
            await file.WriteAsync(indexBytes, cancellationToken);

            var footer = new byte[Sst.FooterLength];
            Sst.Magic.CopyTo(footer);
            BinaryPrimitives.WriteUInt64LittleEndian(footer.AsSpan(8, 8), minOffset);
            BinaryPrimitives.WriteUInt64LittleEndian(footer.AsSpan(16, 8), maxOffset);
            BinaryPrimitives.WriteUInt64LittleEndian(footer.AsSpan(24, 8), (ulong)indexOffset);
            BinaryPrimitives.WriteUInt32LittleEndian(footer.AsSpan(32, 4), (uint)indexBytes.Length);
            await file.WriteAsync(footer, cancellationToken);

            file.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path);
    }
}
