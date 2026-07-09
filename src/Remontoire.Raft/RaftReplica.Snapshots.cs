using System.Diagnostics;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Remontoire.Raft.V1;
using Remontoire.Storage;

namespace Remontoire.Raft;

public sealed partial class RaftReplica {
    /// <inheritdoc cref="ReceiveVoteRequestAsync"/>
    public ValueTask<InstallSnapshotResponse> ReceiveInstallSnapshotAsync(InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        var reply = new TaskCompletionSource<InstallSnapshotResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var accepted = _channel.Writer.TryWrite(new InstallSnapshotReceived(request, reply));
        ObjectDisposedException.ThrowIf(!accepted, this);

        return new ValueTask<InstallSnapshotResponse>(reply.Task.WaitAsync(cancellationToken));
    }

    // One chunk of an ongoing (or starting) transfer. Chunks are trusted to arrive in order for
    // a given (Term, LastIncludedIndex) transfer — the leader only ever sends the next chunk
    // after this RPC's reply for the previous one came back, the same sequencing AppendEntries
    // already relies on; there is no out-of-order/retransmission handling beyond that.
    async Task HandleInstallSnapshotReceivedAsync(InstallSnapshotReceived message) {
        var request = message.Request;

        // (1) A stale leader is refused with our term — same as AppendEntries/RequestVote.
        if (request.Term < _currentTerm) {
            message.Reply.TrySetResult(new InstallSnapshotResponse { Term = _currentTerm });
            return;
        }

        // (2) Equal-or-higher term: this is the current leader — this RPC counts as a heartbeat,
        // so a long transfer never trips this replica's own election timeout.
        if (request.Term > _currentTerm || _role != ReplicaRole.Follower) {
            await BecomeFollowerAsync(request.Term, request.LeaderId);
        } else {
            Volatile.Write(ref _leaderHint, request.LeaderId);
            RestartElectionTimer();
        }

        // (3) A different LastIncludedIndex than whatever's in progress means a new transfer —
        // discard any partial state from before (a stale or superseded one).
        if (_snapshotInstall is not { } install || install.LastIncludedIndex != request.LastIncludedIndex) {
            _snapshotInstall?.Dispose();
            install = new SnapshotInstallState(request.LastIncludedIndex, request.LastIncludedTerm, request.NextLogicalOffset, replicaConfig.ResolvedSnapshotStagingDirectory);
            Directory.CreateDirectory(install.StagingDirectory);
            _snapshotInstall = install;
        }

        // An empty snapshot (nothing was ever flushed to a segment — a shard with no Append
        // entries at all is a real, valid case, not just an edge case) carries no file_name at
        // all: a bare chunk with only Done set, no file I/O to do here.
        if (request.FileName.Length > 0) {
            if (install.CurrentFileName != request.FileName) {
                install.CloseCurrentFile();
                install.CurrentFileName = request.FileName;
                install.CurrentFileStream = new FileStream(
                    Path.Combine(install.StagingDirectory, request.FileName), FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 0, useAsync: true);
            }

            await install.CurrentFileStream!.WriteAsync(request.Data.Memory);

            if (request.FileDone) {
                install.CompletedFilePaths.Add(Path.Combine(install.StagingDirectory, request.FileName));
                await install.CurrentFileStream.DisposeAsync();
                install.CurrentFileStream = null;
                install.CurrentFileName = null;
            }
        }

        if (request.Done) {
            await FinishSnapshotInstallAsync(install);
            _snapshotInstall = null;
        }

        message.Reply.TrySetResult(new InstallSnapshotResponse { Term = _currentTerm });
    }

    // Persist before installing the new log base — same crash-safety reasoning as
    // HandleSnapshotPreparedAsync: a crash in between leaves SnapshotNextLogicalOffset ahead of
    // raftLog.SnapshotIndex, which RecoverNextLogicalOffsetAsync's scan still covers safely.
    async Task FinishSnapshotInstallAsync(SnapshotInstallState install) {
        _snapshotNextLogicalOffset = install.NextLogicalOffset;

        // Persists whatever _activeConfiguration already is — the InstallSnapshot wire protocol
        // carries no membership data of its own. A membership change compacted away on the
        // leader before this replica ever saw it stays unrecoverable through this path; only a
        // later ShardConfigChange (ordinary replication, once this replica resumes past
        // install.LastIncludedIndex) can correct it. Accepted for fase 3: membership changes are
        // rare, one-at-a-time, deliberate operator actions — unlike the data path.
        _snapshotConfiguration = SerializeSnapshotConfiguration();
        await stateStore.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor, _snapshotNextLogicalOffset, _snapshotConfiguration));
        await raftLog.InstallSnapshotAsync(install.LastIncludedIndex, install.LastIncludedTerm);

        if (installSnapshot is not null)
            await installSnapshot(install.CompletedFilePaths, install.NextLogicalOffset, _cts?.Token ?? CancellationToken.None);

        // The snapshot's own base is, by construction, already quorum-committed on the leader
        // that sent it — this replica now has durable proof of that, even though it never saw
        // the individual entries.
        if (install.LastIncludedIndex > _commitIndex)
            Volatile.Write(ref _commitIndex, install.LastIncludedIndex);
    }

    // The leader side. Called from SendAppendEntriesAsync when a peer has fallen behind
    // SnapshotIndex; runs the whole multi-chunk transfer as one background task (never blocking
    // the actor loop on file or network I/O) and reports back exactly once, after the last
    // chunk's response — not once per chunk, unlike AppendEntries' per-RPC reporting.
    async Task SendInstallSnapshotAsync(string peerId) {
        if (prepareSnapshot is null || _installSnapshotInProgressPeers!.Contains(peerId))
            return; // nothing to serve, or a transfer to this peer is already in flight

        _installSnapshotInProgressPeers.Add(peerId);

        var term = _currentTerm;
        var lastIncludedIndex = raftLog.SnapshotIndex;
        var lastIncludedTerm = raftLog.SnapshotTerm;
        var nextLogicalOffset = _snapshotNextLogicalOffset;
        var groupId = replicaConfig.GroupId;
        var nodeId = replicaConfig.NodeId;
        var chunkSizeBytes = replicaConfig.SnapshotChunkSizeBytes;
        var cancellationToken = _cts?.Token ?? CancellationToken.None;

        _ = Task.Run(async () => {
            try {
                var response = await SendAllChunksAsync(
                    peerId, term, lastIncludedIndex, lastIncludedTerm, nextLogicalOffset, groupId, nodeId, chunkSizeBytes, cancellationToken);
                _channel.Writer.TryWrite(new InstallSnapshotResponseReceived(peerId, response, term, lastIncludedIndex));
            } catch (Exception ex) {
                logger?.LogDebug(ex, "InstallSnapshot to {PeerId} failed in {ShardGroupId}", peerId, GroupId);
                _channel.Writer.TryWrite(new InstallSnapshotTransferFailed(peerId));
            }
        }, cancellationToken);
    }

    // Sends every segment as a sequence of chunks and returns the response to the last one sent
    // — returns early, without sending the rest, the moment a response reports a higher term:
    // there is no point continuing to a peer (or cluster) that has moved on.
    async Task<InstallSnapshotResponse> SendAllChunksAsync(
        string peerId, ulong term, ulong lastIncludedIndex, ulong lastIncludedTerm, ulong nextLogicalOffset,
        string groupId, string nodeId, int chunkSizeBytes, CancellationToken cancellationToken) {
        var segmentPaths = await prepareSnapshot!(nextLogicalOffset, cancellationToken);

        // A snapshot with nothing ever flushed to a segment (no Append entries at all in its
        // range) still needs exactly one chunk sent — Done alone, no file — so the peer's actor
        // still runs FinishSnapshotInstallAsync and actually catches up.
        if (segmentPaths.Count == 0) {
            var emptyRequest = new InstallSnapshotRequest {
                GroupId = groupId, Term = term, LeaderId = nodeId,
                LastIncludedIndex = lastIncludedIndex, LastIncludedTerm = lastIncludedTerm, NextLogicalOffset = nextLogicalOffset,
                Done = true,
            };
            return await transport.InstallSnapshotAsync(peerId, emptyRequest, cancellationToken);
        }

        for (var fileIndex = 0; fileIndex < segmentPaths.Count; fileIndex++) {
            var isLastFile = fileIndex == segmentPaths.Count - 1;
            await foreach (var (chunk, fileDone) in ReadChunksAsync(segmentPaths[fileIndex], chunkSizeBytes, cancellationToken)) {
                var request = new InstallSnapshotRequest {
                    GroupId = groupId,
                    Term = term,
                    LeaderId = nodeId,
                    LastIncludedIndex = lastIncludedIndex,
                    LastIncludedTerm = lastIncludedTerm,
                    NextLogicalOffset = nextLogicalOffset,
                    FileName = Path.GetFileName(segmentPaths[fileIndex]),
                    Data = ByteString.CopyFrom(chunk.Span),
                    FileDone = fileDone,
                    Done = fileDone && isLastFile,
                };

                var response = await transport.InstallSnapshotAsync(peerId, request, cancellationToken);
                if (response.Term > term || request.Done)
                    return response; // a higher term (moved on) or genuinely the last chunk sent
            }
        }

        throw new UnreachableException("segmentPaths is non-empty, so the loop above always returns via its last chunk.");
    }

    static async IAsyncEnumerable<(ReadOnlyMemory<byte> Chunk, bool FileDone)> ReadChunksAsync(
        string path, int chunkSizeBytes, [EnumeratorCancellation] CancellationToken cancellationToken) {
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 0, useAsync: true);
        var buffer = new byte[chunkSizeBytes];
        var remaining = file.Length;

        while (remaining > 0) {
            var toRead = (int)Math.Min(remaining, chunkSizeBytes);
            var read = await file.ReadAsync(buffer.AsMemory(0, toRead), cancellationToken);
            remaining -= read;
            yield return (buffer.AsMemory(0, read), remaining == 0);
        }

        if (file.Length == 0)
            yield return (ReadOnlyMemory<byte>.Empty, true); // an empty segment file is unusual but not invalid — still needs a FileDone chunk
    }

    async Task HandleInstallSnapshotResponseReceivedAsync(InstallSnapshotResponseReceived message) {
        _installSnapshotInProgressPeers?.Remove(message.PeerId);

        if (message.SentTerm != _currentTerm || _role != ReplicaRole.Leader)
            return; // stale by construction

        if (message.Response.Term > _currentTerm) {
            await BecomeFollowerAsync(message.Response.Term, leaderHint: null);
            return;
        }

        // The membership can now change mid-term (RaftReplica.Membership.cs) — a since-applied
        // ShardConfigChange may already have dropped this peer. The dictionary indexer's setter
        // would otherwise silently resurrect a departed peer's entry rather than throw.
        if (!_nextIndex!.ContainsKey(message.PeerId))
            return;

        // A successful transfer means the peer is now caught up to the snapshot's own base —
        // resume ordinary AppendEntries replication for anything above it.
        _nextIndex[message.PeerId] = message.SentSnapshotIndex + 1;
        _matchIndex![message.PeerId] = message.SentSnapshotIndex;
        await TryAdvanceLeaderCommitAsync();
        await SendAppendEntriesAsync(message.PeerId);
    }

    Task HandleInstallSnapshotTransferFailedAsync(InstallSnapshotTransferFailed message) {
        _installSnapshotInProgressPeers?.Remove(message.PeerId); // the next heartbeat tick retries from scratch
        return Task.CompletedTask;
    }

    // Called after every commit advance (both roles — a follower bounds its own WAL exactly like
    // a leader does). Cheap to call unconditionally: every guard below is an in-memory or
    // already-durable-log check, no I/O, until the point where a round trip actually starts.
    async Task TryTriggerSnapshotAsync() {
        if (prepareSnapshot is null || _snapshotInProgress)
            return;

        if (raftLog.LastIndex - raftLog.SnapshotIndex <= replicaConfig.SnapshotThresholdEntries)
            return;

        var lastIncludedIndex = _commitIndex;
        if (lastIncludedIndex <= raftLog.SnapshotIndex)
            return; // nothing newly committed since the last snapshot yet

        _snapshotInProgress = true;

        var lastIncludedTerm = await raftLog.GetTermAtAsync(lastIncludedIndex);

        // The LogicalOffset this snapshot's base sits at — same derivation as
        // RecoverNextLogicalOffsetAsync, bounded to this snapshot's range instead of the full tail.
        var nextLogicalOffset = _snapshotNextLogicalOffset;
        await foreach (var record in raftLog.ReadFromAsync(raftLog.SnapshotIndex + 1)) {
            if (record.RaftIndex > lastIncludedIndex)
                break;
            if (record.RecordType == WalRecordType.Append)
                nextLogicalOffset = record.LogicalOffset + 1;
        }

        var cancellationToken = _cts?.Token ?? CancellationToken.None;
        _ = Task.Run(async () => {
            try {
                // Ensures everything below nextLogicalOffset is durable in a segment — the
                // returned paths matter only to an InstallSnapshot serve, not to compaction
                // itself, so they're discarded here; a later serve re-queries fresh instead of
                // caching this result.
                await prepareSnapshot(nextLogicalOffset, cancellationToken);
                _channel.Writer.TryWrite(new SnapshotPrepared(lastIncludedIndex, lastIncludedTerm, nextLogicalOffset));
            } catch (Exception ex) {
                logger?.LogWarning(ex, "Snapshot preparation failed in {ShardGroupId}", GroupId);
                _channel.Writer.TryWrite(new SnapshotPreparationFailed());
            }
        }, cancellationToken);
    }

    // Persist before compact — a crash in between leaves SnapshotNextLogicalOffset ahead of
    // raftLog.SnapshotIndex, which is safe (RecoverNextLogicalOffsetAsync's scan still covers
    // the gap); the reverse order is not.
    async Task HandleSnapshotPreparedAsync(SnapshotPrepared message) {
        _snapshotInProgress = false;
        _snapshotNextLogicalOffset = message.NextLogicalOffset;
        _snapshotConfiguration = SerializeSnapshotConfiguration(); // whatever is active right now
        await stateStore.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor, _snapshotNextLogicalOffset, _snapshotConfiguration));
        await raftLog.CompactToAsync(message.LastIncludedIndex, message.LastIncludedTerm);
    }

    Task HandleSnapshotPreparationFailedAsync(SnapshotPreparationFailed message) {
        _snapshotInProgress = false; // a later commit advance gets to try again
        return Task.CompletedTask;
    }
}

/// <summary>
/// One <c>InstallSnapshot</c> chunk-reassembly in progress on the receiving side. Lives on the
/// actor rather than a role object so it survives role transitions within the same term — see
/// <see cref="RaftReplica.BecomeFollowerAsync"/>/<c>BecomeCandidateAsync</c>, which dispose and
/// clear it on every term change instead.
/// </summary>
sealed class SnapshotInstallState(ulong lastIncludedIndex, ulong lastIncludedTerm, ulong nextLogicalOffset, string stagingDirectory) : IDisposable {
    public ulong LastIncludedIndex { get; } = lastIncludedIndex;
    public ulong LastIncludedTerm { get; } = lastIncludedTerm;
    public ulong NextLogicalOffset { get; } = nextLogicalOffset;
    public string StagingDirectory { get; } = stagingDirectory;
    public List<string> CompletedFilePaths { get; } = [];
    public string? CurrentFileName { get; set; }
    public FileStream? CurrentFileStream { get; set; }

    public void CloseCurrentFile() {
        CurrentFileStream?.Dispose();
        CurrentFileStream = null;
        CurrentFileName = null;
    }

    public void Dispose() => CurrentFileStream?.Dispose();
}
