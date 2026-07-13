using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Remontoire.Storage.Compaction;

namespace Remontoire.Storage;

/// <summary>
/// The single public entry point into a shard's durable log — WAL, MemTable, and SST segments
/// combined behind one API. A reader never knows (or needs to know) whether a given offset is
/// still in the MemTable or already flushed to a segment.
/// </summary>
public sealed partial class ShardLog : IAsyncDisposable {
    readonly string _directory;
    readonly long _flushThresholdBytes;
    readonly Func<CancellationToken, IAsyncEnumerable<WalRecord>> _committedSource;
    readonly CancellationTokenSource _tailingCts = new();
    readonly Task _tailingLoop;
    readonly Task _actorLoop;
    readonly Channel<ShardLogMessage> _mailbox = Channel.CreateUnbounded<ShardLogMessage>(new UnboundedChannelOptions { SingleReader = true });
    readonly Channel<WalRecord> _applied = Channel.CreateUnbounded<WalRecord>(new UnboundedChannelOptions { SingleReader = true });
    readonly CompactionPolicy? _compactionPolicy;
    readonly Task? _compactionWorkerLoop;
    readonly RetentionPolicy? _retentionPolicy;
    readonly CancellationTokenSource _retentionCts = new();
    readonly Task? _retentionTickerLoop;
    readonly SizePruneWorker? _sizePruneWorker;
    readonly Task? _sizePruneWorkerLoop;

    MemTable _memTable;
    SstSegment[] _segments;
    ulong _nextOffsetToApply;
    TaskCompletionSource<CompactionPlan>? _pendingPlanRequest;
    readonly List<PrepareSnapshotRequested> _pendingSnapshotRequests = [];

    // A signal, not a data channel — completed and replaced every time an Append is applied, so
    // any number of concurrent ConsumeAsync-style waiters can each await it independently. The
    // waiter re-reads (TryGet/ReadFromAsync) after it completes; this never carries the record
    // itself, only "something changed, look again."
    volatile TaskCompletionSource _appended = new(TaskCreationOptions.RunContinuationsAsynchronously);

    readonly ILogger? _logger;

    internal ShardLog(
        string directory, Func<CancellationToken, IAsyncEnumerable<WalRecord>> committedSource, MemTable memTable, SstSegment[] segments,
        ulong nextOffsetToApply, long flushThresholdBytes, CompactionPolicy? compactionPolicy = null, RetentionPolicy? retentionPolicy = null,
        ILogger? logger = null) {
        _directory = directory;
        _committedSource = committedSource;
        _memTable = memTable;
        _segments = segments;
        _nextOffsetToApply = nextOffsetToApply;
        _flushThresholdBytes = flushThresholdBytes;
        _compactionPolicy = compactionPolicy;
        _retentionPolicy = retentionPolicy;
        _logger = logger;

        _actorLoop = Task.Run(RunActorAsync);
        _tailingLoop = Task.Run(RunTailingLoopAsync);
        _compactionWorkerLoop = compactionPolicy is null ? null : Task.Run(new CompactionWorker(_mailbox.Writer, compactionPolicy.OnCompactionDurationMeasured).RunAsync);
        _retentionTickerLoop = compactionPolicy?.GetAckedLowWatermarkAsync is null ? null
            : Task.Run(() => RunRetentionTickerAsync(compactionPolicy.RetentionTickInterval, _retentionCts.Token));
        _sizePruneWorker = retentionPolicy is null ? null
            : new SizePruneWorker(directory, retentionPolicy.GetMaxTotalBytesPerVirtualShard, retentionPolicy.IsAdmissionPaused, _mailbox.Writer,
                tickInterval: retentionPolicy.SizePruneTickInterval, logger: logger);
        _sizePruneWorkerLoop = _sizePruneWorker is null ? null : Task.Run(() => _sizePruneWorker.RunAsync(_retentionCts.Token));
    }

    /// <summary>
    /// Posts <paramref name="message"/> directly to the actor's mailbox — lets tests inject a
    /// committed record (or a compaction message) without a real committed-source.
    /// </summary>
    internal bool TryPost(ShardLogMessage message) => _mailbox.Writer.TryWrite(message);

    /// <summary>
    /// One past the highest logical offset this log has actually applied — callers can use this
    /// as an upper bound to reject offsets that don't refer to any message that actually exists yet.
    /// </summary>
    public ulong NextOffsetToApply => Volatile.Read(ref _nextOffsetToApply);

    /// <summary>
    /// Running count of messages forcibly pruned by the size-based emergency floor — mirrors how
    /// <see cref="Compaction.CompactionWorker"/> is wrapped without being exposed directly.
    /// Every increase here is a guarantee-break (a message discarded regardless of ack status),
    /// never routine, expected pruning. Zero when this log has no <see cref="RetentionPolicy"/>.
    /// </summary>
    public long ForcedPruneMessagesTotal => _sizePruneWorker?.MessagesPrunedTotal ?? 0;

    /// <summary>
    /// Shuts down in the order that guarantees every message already sitting in the mailbox is
    /// applied before anything stops. The committed-source itself is owned by its caller — this
    /// never disposes or completes it, only stops reading from it.
    /// </summary>
    public async ValueTask DisposeAsync() {
        // Stop pulling new records from the committed-source first — nothing new can reach the
        // mailbox after this, so completing it below can't race with the tailing loop.
        await _tailingCts.CancelAsync();
        await _tailingLoop;
        _tailingCts.Dispose();

        // Both are Task.Delay-driven, not blocked on any actor reply, so — unlike the compaction
        // worker below — they need their own cancellation to stop; each also still posts to the
        // mailbox, so this must happen before it's completed, same reason as the tailing loop above.
        await _retentionCts.CancelAsync();
        if (_retentionTickerLoop is not null)
            await _retentionTickerLoop;
        if (_sizePruneWorkerLoop is not null)
            await _sizePruneWorkerLoop;
        _retentionCts.Dispose();

        _mailbox.Writer.TryComplete();
        await _actorLoop;

        // The compaction worker stops on its own once the mailbox closes (its next TryWrite
        // fails) or its outstanding plan request gets cancelled (RunActorAsync's cleanup below)
        // — no separate cancellation token needed.
        if (_compactionWorkerLoop is not null)
            await _compactionWorkerLoop;

        foreach (var segment in Volatile.Read(ref _segments))
            segment.Dispose();
    }

    // One shared mailbox for everything that mutates this shard's live state: newly-committed
    // records (from the tailing loop, which only ever posts — it never touches
    // _memTable/_segments itself) and compaction messages. All are just messages, processed one
    // at a time in strict arrival order, so none can ever interleave in a way that corrupts
    // ordering or applies things out of sequence.
    async Task RunActorAsync() {
        await foreach (var message in _mailbox.Reader.ReadAllAsync()) {
            switch (message) {
                case WalRecordCommitted committed:
                    await HandleWalRecordCommittedAsync(committed);
                    break;
                case CompactionPlanRequest request:
                    HandleCompactionPlanRequest(request);
                    break;
                case CompactionCompleted completed:
                    HandleCompactionCompleted(completed);
                    break;
                case PrepareSnapshotRequested prepareSnapshotRequested:
                    await HandlePrepareSnapshotRequestedAsync(prepareSnapshotRequested);
                    break;
                case SnapshotInstalled snapshotInstalled:
                    await HandleSnapshotInstalledAsync(snapshotInstalled);
                    break;
                case RetentionPassRequested:
                    await HandleRetentionPassRequestedAsync();
                    break;
                case SizePruneCompleted sizePruneCompleted:
                    HandleSizePruneCompleted(sizePruneCompleted);
                    break;
            }
        }

        // No outstanding request may block a caller forever when ShardLog shuts down mid-flight.
        _pendingPlanRequest?.TrySetCanceled();
        foreach (var pending in _pendingSnapshotRequests)
            pending.Completion.TrySetCanceled();
    }
}
