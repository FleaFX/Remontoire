using System.Buffers.Binary;
using System.Text;

namespace Remontoire.Raft;

/// <summary>
/// A file-backed <see cref="IRaftStateStore"/> — one small file per group, written atomically
/// (temp + fsync + rename, same discipline as <see cref="SnapshotMarker"/> and every other small
/// durable file in this codebase).
/// </summary>
public sealed class FileRaftStateStore(string directory) : IRaftStateStore {
    const string FileName = "raft-state.dat";
    static ReadOnlySpan<byte> Magic => "RMTRSTAT"u8;

    /// <inheritdoc />
    public async ValueTask<RaftPersistentState> LoadAsync(CancellationToken cancellationToken = default) {
        var path = Path.Combine(directory, FileName);
        if (!File.Exists(path))
            return new RaftPersistentState(CurrentTerm: 0, VotedFor: null);

        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        var span = bytes.AsSpan();

        if (span.Length < 8 || !span[..8].SequenceEqual(Magic))
            throw new InvalidDataException($"'{path}' is not a valid Raft state file.");
        span = span[8..];

        var currentTerm = BinaryPrimitives.ReadUInt64LittleEndian(span);
        span = span[8..];

        var votedFor = ReadString(ref span);

        var snapshotNextLogicalOffset = BinaryPrimitives.ReadUInt64LittleEndian(span);
        span = span[8..];

        var snapshotConfiguration = ReadBytes(ref span);

        return new RaftPersistentState(currentTerm, votedFor, snapshotNextLogicalOffset, snapshotConfiguration);
    }

    /// <inheritdoc />
    public async ValueTask SaveAsync(RaftPersistentState state, CancellationToken cancellationToken = default) {
        Directory.CreateDirectory(directory);

        var path = Path.Combine(directory, FileName);
        var tempPath = path + ".tmp";

        var votedForBytes = state.VotedFor is null ? null : Encoding.UTF8.GetBytes(state.VotedFor);
        var length = Magic.Length + 8 + 4 + (votedForBytes?.Length ?? 0) + 8 + 4 + (state.SnapshotConfiguration?.Length ?? 0);

        var buffer = new byte[length];
        var span = buffer.AsSpan();

        Magic.CopyTo(span);
        span = span[Magic.Length..];

        BinaryPrimitives.WriteUInt64LittleEndian(span, state.CurrentTerm);
        span = span[8..];

        span = WriteBytes(span, votedForBytes);

        BinaryPrimitives.WriteUInt64LittleEndian(span, state.SnapshotNextLogicalOffset);
        span = span[8..];

        WriteBytes(span, state.SnapshotConfiguration);

        await using (var file = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 0, useAsync: true)) {
            await file.WriteAsync(buffer, cancellationToken);
            file.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
    }

    // -1 (as uint32) marks "absent" (null), distinct from a present-but-empty byte sequence.
    static Span<byte> WriteBytes(Span<byte> span, byte[]? value) {
        BinaryPrimitives.WriteInt32LittleEndian(span, value?.Length ?? -1);
        span = span[4..];

        if (value is null)
            return span;

        value.CopyTo(span);
        return span[value.Length..];
    }

    static byte[]? ReadBytes(ref Span<byte> span) {
        var length = BinaryPrimitives.ReadInt32LittleEndian(span);
        span = span[4..];

        if (length < 0)
            return null;

        var value = span[..length].ToArray();
        span = span[length..];
        return value;
    }

    static string? ReadString(ref Span<byte> span) {
        var bytes = ReadBytes(ref span);
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }
}
