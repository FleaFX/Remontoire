using System.Buffers.Binary;

namespace Remontoire.Raft;

/// <summary>
/// The durable record of <see cref="WalRaftLog"/>'s own compacted base — <c>SnapshotIndex</c>/
/// <c>SnapshotTerm</c>, written atomically (temp + fsync + rename, same discipline as every
/// other small durable file in this codebase) once per <see cref="WalRaftLog.CompactToAsync"/>
/// call, always before any WAL file that call deletes actually disappears — a crash in between
/// just means some already-covered files are still on disk next time, harmless, cleaned up by
/// <see cref="WalRaftLog.OpenAsync"/>'s own recovery pass.
/// </summary>
static class SnapshotMarker {
    const string FileName = "snapshot.marker";
    const int Length = 24; // magic(8) + index(8) + term(8)
    static ReadOnlySpan<byte> Magic => "RMTRSNAP"u8;

    /// <summary>
    /// Loads the marker in <paramref name="directory"/>, or <c>(0, 0)</c> when none exists yet
    /// (no snapshot has ever been taken).
    /// </summary>
    public static async Task<(ulong Index, ulong Term)> LoadAsync(string directory, CancellationToken cancellationToken = default) {
        var path = Path.Combine(directory, FileName);
        if (!File.Exists(path))
            return (0, 0);

        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        if (bytes.Length != Length || !bytes.AsSpan(0, 8).SequenceEqual(Magic))
            throw new InvalidDataException($"'{path}' is not a valid snapshot marker.");

        return (BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8)), BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(16, 8)));
    }

    /// <summary>
    /// Durably persists the marker in <paramref name="directory"/> before returning.
    /// </summary>
    public static async Task SaveAsync(string directory, ulong index, ulong term, CancellationToken cancellationToken = default) {
        var path = Path.Combine(directory, FileName);
        var tempPath = path + ".tmp";

        var buffer = new byte[Length];
        Magic.CopyTo(buffer);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(8, 8), index);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(16, 8), term);

        await using (var file = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 0, useAsync: true)) {
            await file.WriteAsync(buffer, cancellationToken);
            file.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
    }
}
