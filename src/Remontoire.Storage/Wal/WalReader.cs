using System.Runtime.CompilerServices;
using Remontoire.Storage.Serialization;

namespace Remontoire.Storage;

/// <summary>
/// Reads <see cref="WalRecord"/>s sequentially from a WAL file. Used for recovery (reading
/// from the start, or from a known-good position) — the only other consumer of WAL content,
/// live-apply, no longer reads the WAL back at all; it consumes <see cref="WalWriter"/>'s own
/// in-memory <see cref="WalWriter.ReadDurableAsync"/> stream instead.
/// </summary>
/// <param name="path">Path to the WAL file to read from.</param>
public sealed class WalReader(string path) {
    /// <summary>
    /// Reads records starting at byte position <paramref name="startPosition"/>, up to
    /// whatever is currently on disk. Stops — without throwing — at the first incomplete or
    /// corrupt record; reaching an incomplete record exactly at end-of-file is the expected,
    /// normal way this enumerable ends when reading a WAL that a writer is still appending to.
    /// The caller must dispose each yielded <see cref="WalReadResult"/> once done with it.
    /// </summary>
    public async IAsyncEnumerable<WalReadResult> ReadFromAsync(long startPosition = 0, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        byte[] tail;

        await using (var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 0, useAsync: true)) {
            file.Position = startPosition;
            tail = new byte[file.Length - startPosition];
            await file.ReadExactlyAsync(tail, cancellationToken);
        }

        var offset = 0;
        while (offset < tail.Length) {
            cancellationToken.ThrowIfCancellationRequested();

            var result = WalRecordSerializer.TryRead(tail.AsSpan(offset));
            if (result.Status != WalRecordReadStatus.Success) {
                result.Dispose();
                yield break;
            }

            yield return result;
            offset += result.BytesConsumed;
        }
    }
}
