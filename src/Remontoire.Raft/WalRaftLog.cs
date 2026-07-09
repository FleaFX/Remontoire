using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Remontoire.Storage;
using Remontoire.Storage.Serialization;

namespace Remontoire.Raft;

/// <summary>
/// The production <see cref="IRaftLog"/> — owns a sequence of <c>wal-{FirstIndex}.log</c> files
/// in one directory (the same naming convention as <c>SstSegment</c>'s <c>segment-{offset}.sst</c>),
/// one active (still being appended to) plus zero or more sealed (rotated out, read-only), and a
/// live-maintained <c>RaftIndex → (file, byte position)</c> index so <see cref="GetTermAtAsync"/>/
/// <see cref="ReadFromAsync"/> seek straight to the right spot instead of scanning from the start.
/// This directory has exactly one writer (this instance), so positions can be computed
/// synchronously ahead of each write and only committed to the index once that write is
/// confirmed durable — no reliance on read-back ordering.
/// </summary>
public sealed class WalRaftLog : IRaftLog, IAsyncDisposable {
    const string FilePrefix = "wal-";
    const string FileExtension = ".log";

    readonly string _directory;
    readonly long _rotationThresholdBytes;
    readonly List<WalFileRange> _sealedFiles;
    readonly Dictionary<ulong, (string Path, long Position)> _positionByIndex;

    WalWriter _activeWriter;
    string _activePath;
    ulong _activeFirstIndex;
    long _nextWritePosition;
    ulong _lastIndex;
    ulong _lastTerm;

    WalRaftLog(
        string directory, long rotationThresholdBytes, List<WalFileRange> sealedFiles, Dictionary<ulong, (string Path, long Position)> positionByIndex,
        WalWriter activeWriter, string activePath, ulong activeFirstIndex, long nextWritePosition, ulong lastIndex, ulong lastTerm) {
        _directory = directory;
        _rotationThresholdBytes = rotationThresholdBytes;
        _sealedFiles = sealedFiles;
        _positionByIndex = positionByIndex;
        _activeWriter = activeWriter;
        _activePath = activePath;
        _activeFirstIndex = activeFirstIndex;
        _nextWritePosition = nextWritePosition;
        _lastIndex = lastIndex;
        _lastTerm = lastTerm;
    }

    /// <inheritdoc />
    public ulong LastIndex => _lastIndex;

    /// <inheritdoc />
    public ulong LastTerm => _lastTerm;

    /// <inheritdoc />
    public ulong SnapshotIndex { get; private set; }

    /// <inheritdoc />
    public ulong SnapshotTerm { get; private set; }

    /// <summary>
    /// Opens (creating if necessary) the WAL in <paramref name="directory"/>, rebuilding the
    /// position index with a single sequential scan per remaining file — the same
    /// torn-write-stops-cleanly ending as the storage layer's own recovery: whatever a scan
    /// can't fully read back was never durable. Also finishes any compaction an earlier crash
    /// left half-done: a sealed file the snapshot marker already covers, but that never got
    /// deleted, is deleted here instead of scanned.
    /// </summary>
    public static async Task<WalRaftLog> OpenAsync(string directory, long rotationThresholdBytes = 64 * 1024 * 1024, CancellationToken cancellationToken = default) {
        Directory.CreateDirectory(directory);
        var (snapshotIndex, snapshotTerm) = await SnapshotMarker.LoadAsync(directory, cancellationToken);

        var files = ListWalFilesInOrder(directory);
        if (files.Count == 0) {
            var freshPath = WalFilePath(directory, snapshotIndex + 1);
            var freshWriter = await WalWriter.OpenAsync(freshPath, cancellationToken);
            var fresh = new WalRaftLog(directory, rotationThresholdBytes, [], [], freshWriter, freshPath, snapshotIndex + 1, 0, snapshotIndex, snapshotTerm);
            fresh.SnapshotIndex = snapshotIndex;
            fresh.SnapshotTerm = snapshotTerm;
            return fresh;
        }

        var sealedFiles = new List<WalFileRange>();
        var positionByIndex = new Dictionary<ulong, (string Path, long Position)>();
        var lastIndex = snapshotIndex;
        var lastTerm = snapshotTerm;

        for (var i = 0; i < files.Count; i++) {
            var path = files[i];
            var isLastFile = i == files.Count - 1;

            // Ranges are contiguous and non-overlapping by construction (rotation always starts
            // the next file at lastIndex + 1) — a sealed file's own LastIndex is therefore always
            // exactly one below the *next* file's FirstIndex, readable from that file's name
            // alone. Only the active file's LastIndex is unknown ahead of a scan.
            var inferredLastIndex = isLastFile ? (ulong?)null : ParseFirstIndex(files[i + 1]) - 1;

            if (inferredLastIndex is { } coveredLastIndex && coveredLastIndex <= snapshotIndex) {
                File.Delete(path); // an interrupted compaction, finished now — never scanned
                continue;
            }

            var firstIndexInFile = ParseFirstIndex(path);
            long positionInFile = 0;

            var reader = new WalReader(path);
            await foreach (var result in reader.ReadFromAsync(0, cancellationToken)) {
                using (result) {
                    positionByIndex[result.Record.RaftIndex] = (path, positionInFile);
                    positionInFile += result.BytesConsumed;
                    lastIndex = result.Record.RaftIndex;
                    lastTerm = result.Record.RaftTerm;
                }
            }

            if (isLastFile) {
                var writer = await WalWriter.OpenAsync(path, cancellationToken);
                var log = new WalRaftLog(directory, rotationThresholdBytes, sealedFiles, positionByIndex, writer, path, firstIndexInFile, positionInFile, lastIndex, lastTerm);
                log.SnapshotIndex = snapshotIndex;
                log.SnapshotTerm = snapshotTerm;
                return log;
            }

            sealedFiles.Add(new WalFileRange(path, firstIndexInFile, lastIndex));
        }

        throw new UnreachableException("files.Count > 0 guarantees the loop above always returns via the last file.");
    }

    /// <inheritdoc />
    public async ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken cancellationToken = default) {
        if (index == SnapshotIndex)
            return SnapshotTerm;

        await foreach (var record in ReadFromAsync(index, cancellationToken))
            return record.RaftTerm;

        throw new InvalidOperationException($"No entry at index {index}.");
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<WalRecord> ReadFromAsync(ulong fromIndex, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        if (fromIndex > _lastIndex)
            yield break;

        var files = AllFileRanges();
        var startFile = 0;
        for (var i = files.Count - 1; i >= 0; i--) {
            if (fromIndex >= files[i].FirstIndex) {
                startFile = i;
                break;
            }
        }

        for (var i = startFile; i < files.Count; i++) {
            var startPosition = i == startFile ? _positionByIndex[fromIndex].Position : 0;
            var reader = new WalReader(files[i].Path);
            await foreach (var result in reader.ReadFromAsync(startPosition, cancellationToken)) {
                using (result)
                    yield return result.Record;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask AppendAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default) {
        if (entries.Count == 0)
            return;

        // Positions are computed up front, against the current tail — nothing but this call
        // writes to this file, so the layout of this batch is fully known before any byte hits
        // disk. Held locally, not applied to `_positionByIndex`/`_nextWritePosition` until every
        // entry is confirmed durable below: a failed append must leave no trace in the index.
        var activePath = _activePath;
        var positions = new (ulong Index, long Position)[entries.Count];
        var position = _nextWritePosition;
        for (var i = 0; i < entries.Count; i++) {
            positions[i] = (entries[i].RaftIndex, position);
            position += WalRecordSerializer.GetEncodedLength(entries[i]);
        }

        var appends = new Task[entries.Count];
        for (var i = 0; i < entries.Count; i++)
            appends[i] = _activeWriter.AppendAsync(entries[i], cancellationToken).AsTask();
        await Task.WhenAll(appends);

        foreach (var (index, entryPosition) in positions)
            _positionByIndex[index] = (activePath, entryPosition);
        _nextWritePosition = position;

        var last = entries[^1];
        _lastIndex = last.RaftIndex;
        _lastTerm = last.RaftTerm;

        if (_nextWritePosition >= _rotationThresholdBytes)
            await RotateAsync(cancellationToken);
    }

    // Seals the active file (closes it, remembers its now-fixed range) and opens a fresh one for
    // everything from here on. Never touches _positionByIndex — every position already recorded
    // against the now-sealed file's path stays valid; only future appends go to the new one.
    async Task RotateAsync(CancellationToken cancellationToken) {
        await _activeWriter.DisposeAsync();
        _sealedFiles.Add(new WalFileRange(_activePath, _activeFirstIndex, _lastIndex));

        var newFirstIndex = _lastIndex + 1;
        var newPath = WalFilePath(_directory, newFirstIndex);
        _activeWriter = await WalWriter.OpenAsync(newPath, cancellationToken);
        _activePath = newPath;
        _activeFirstIndex = newFirstIndex;
        _nextWritePosition = 0;
    }

    /// <inheritdoc />
    public async ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken cancellationToken = default) {
        var (containingPath, truncateAt) = _positionByIndex[fromIndex];

        if (containingPath == _activePath) {
            // The common case: the conflicting suffix never left the active file.
            await _activeWriter.TruncateFromAsync(truncateAt, cancellationToken);
            _nextWritePosition = truncateAt;
        } else {
            // The conflicting suffix reaches into an already-rotated, sealed file (possible when
            // an uncommitted tail happened to trigger a rotation before a conflict was
            // discovered). Every file strictly after the containing one — sealed or active — is
            // entirely part of the discarded suffix and is deleted outright; the containing file
            // itself is truncated in place and becomes the active file again.
            var containingFileIndex = _sealedFiles.FindIndex(f => f.Path == containingPath);
            var containingFirstIndex = _sealedFiles[containingFileIndex].FirstIndex;

            await _activeWriter.DisposeAsync();
            File.Delete(_activePath);
            for (var i = _sealedFiles.Count - 1; i > containingFileIndex; i--) {
                File.Delete(_sealedFiles[i].Path);
                _sealedFiles.RemoveAt(i);
            }
            _sealedFiles.RemoveAt(containingFileIndex);

            await using (var raw = new FileStream(containingPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
                raw.SetLength(truncateAt);

            _activeWriter = await WalWriter.OpenAsync(containingPath, cancellationToken);
            _activePath = containingPath;
            _activeFirstIndex = containingFirstIndex;
            _nextWritePosition = truncateAt;
        }

        for (var index = fromIndex; index <= _lastIndex; index++)
            _positionByIndex.Remove(index);

        _lastIndex = fromIndex - 1;
        _lastTerm = _lastIndex == SnapshotIndex ? SnapshotTerm : await GetTermAtAsync(_lastIndex, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask CompactToAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken cancellationToken = default) {
        // Durable before deletion — a crash in between leaves some already-covered files on
        // disk, harmless (OpenAsync's recovery finishes the job); the reverse order could lose
        // track of files that were already deleted.
        await SnapshotMarker.SaveAsync(_directory, lastIncludedIndex, lastIncludedTerm, cancellationToken);
        SnapshotIndex = lastIncludedIndex;
        SnapshotTerm = lastIncludedTerm;

        for (var i = _sealedFiles.Count - 1; i >= 0; i--) {
            if (_sealedFiles[i].LastIndex > lastIncludedIndex)
                continue; // straddles the boundary — left alone until a later snapshot covers it fully

            File.Delete(_sealedFiles[i].Path);
            for (var index = _sealedFiles[i].FirstIndex; index <= _sealedFiles[i].LastIndex; index++)
                _positionByIndex.Remove(index);
            _sealedFiles.RemoveAt(i);
        }
    }

    /// <inheritdoc />
    // Crash-safe by construction, not by explicit cleanup ordering: the new active file and
    // marker are established BEFORE anything old is touched, so a crash midway just leaves old
    // files behind — every one of them has FirstIndex <= lastIncludedIndex by definition (this
    // replica only ever installs a snapshot because it had fallen behind), so OpenAsync's
    // existing "finish an interrupted compaction" cleanup deletes them on the next recovery
    // without ever needing to know their real, no-longer-relevant content.
    public async ValueTask InstallSnapshotAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken cancellationToken = default) {
        var staleWriter = _activeWriter;
        var staleFiles = new List<string>(_sealedFiles.Count + 1) { _activePath };
        staleFiles.AddRange(_sealedFiles.Select(f => f.Path));

        var newFirstIndex = lastIncludedIndex + 1;
        var newPath = WalFilePath(_directory, newFirstIndex);
        var newWriter = await WalWriter.OpenAsync(newPath, cancellationToken);
        await SnapshotMarker.SaveAsync(_directory, lastIncludedIndex, lastIncludedTerm, cancellationToken);

        _sealedFiles.Clear();
        _positionByIndex.Clear();
        _activeWriter = newWriter;
        _activePath = newPath;
        _activeFirstIndex = newFirstIndex;
        _nextWritePosition = 0;
        SnapshotIndex = lastIncludedIndex;
        SnapshotTerm = lastIncludedTerm;
        _lastIndex = lastIncludedIndex;
        _lastTerm = lastIncludedTerm;

        await staleWriter.DisposeAsync();
        foreach (var path in staleFiles)
            File.Delete(path);
    }

    /// <summary>Disposes the active <see cref="WalWriter"/>, draining it to durable completion first.</summary>
    public ValueTask DisposeAsync() => _activeWriter.DisposeAsync();

    List<WalFileRange> AllFileRanges() {
        var all = new List<WalFileRange>(_sealedFiles.Count + 1);
        all.AddRange(_sealedFiles);
        all.Add(new WalFileRange(_activePath, _activeFirstIndex, _lastIndex));
        return all;
    }

    static string WalFilePath(string directory, ulong firstIndex) =>
        Path.Combine(directory, $"{FilePrefix}{firstIndex:D20}{FileExtension}");

    static ulong ParseFirstIndex(string path) =>
        ulong.Parse(Path.GetFileNameWithoutExtension(path).AsSpan(FilePrefix.Length), CultureInfo.InvariantCulture);

    // Lexicographic order matches FirstIndex order here only because it's zero-padded to a fixed
    // width — same trick SstSegment's naming already relies on.
    static List<string> ListWalFilesInOrder(string directory) {
        var files = Directory.GetFiles(directory, $"{FilePrefix}*{FileExtension}");
        Array.Sort(files, StringComparer.Ordinal);
        return [.. files];
    }

    readonly record struct WalFileRange(string Path, ulong FirstIndex, ulong LastIndex);
}
