using System.Threading.Channels;

namespace Remontoire.Storage;

/// <summary>
/// The single public entry point into a shard's durable log — WAL, MemTable, and SST segments
/// combined behind one API. A reader never knows (or needs to know) whether a given offset is
/// still in the MemTable or already flushed to a segment.
/// </summary>
public sealed partial class ShardLog : IAsyncDisposable {
    readonly string _directory;
    readonly long _flushThresholdBytes;
    readonly WalWriter _walWriter;
    readonly Task _tailingLoop;
    readonly Task _actorLoop;
    readonly Channel<ShardLogMessage> _mailbox = Channel.CreateUnbounded<ShardLogMessage>(new UnboundedChannelOptions { SingleReader = true });

    MemTable _memTable;
    SstSegment[] _segments;
    ulong _nextLogicalOffset;

    ShardLog(string directory, WalWriter walWriter, MemTable memTable, SstSegment[] segments, ulong nextLogicalOffset, long flushThresholdBytes) {
        _directory = directory;
        _walWriter = walWriter;
        _memTable = memTable;
        _segments = segments;
        _nextLogicalOffset = nextLogicalOffset;
        _flushThresholdBytes = flushThresholdBytes;

        _actorLoop = Task.Run(RunActorAsync);
        _tailingLoop = Task.Run(RunTailingLoopAsync);
    }

    /// <summary>
    /// Shuts down in the order that guarantees every already-accepted <see cref="AppendAsync"/>
    /// call is durably written before anything else stops.
    /// </summary>
    public async ValueTask DisposeAsync() {
        // Drain the mailbox FIRST, while WalWriter is still alive — every already-accepted
        // AppendCommand must reach HandleAppend (which calls into WalWriter) before WalWriter
        // stops accepting appends. Disposing WalWriter first would risk an AppendCommand still
        // sitting in the mailbox trying to write to an already-disposed WalWriter.
        _mailbox.Writer.TryComplete();
        await _actorLoop;

        // NOW safe: every append the actor accepted has already reached WalWriter. Disposing it
        // drains everything to durable completion, then completes its ReadCommittedAsync
        // stream, which lets the tailing loop end naturally below. Any final WalRecordCommitted
        // messages the tailing loop tries to post after this point are silently dropped (mailbox
        // already closed) — harmless, the next OpenAsync's recovery picks them up from the WAL.
        await _walWriter.DisposeAsync();
        await _tailingLoop;

        foreach (var segment in Volatile.Read(ref _segments))
            segment.Dispose();
    }

    // One shared mailbox for everything that mutates this shard's live state: append requests
    // (from external callers) and newly-committed WAL records (from the tailing loop, which
    // only ever posts — it never touches _memTable/_segments itself). Both are just messages,
    // processed one at a time in strict arrival order, so the two can never interleave in a way
    // that corrupts ordering or applies things out of sequence.
    async Task RunActorAsync() {
        await foreach (var message in _mailbox.Reader.ReadAllAsync()) {
            switch (message) {
                case AppendCommand append:
                    HandleAppend(append);
                    break;
                case WalRecordCommitted committed:
                    await HandleWalRecordCommittedAsync(committed);
                    break;
            }
        }
    }

    abstract record ShardLogMessage;
    sealed record AppendCommand(AppendRequest Request, TaskCompletionSource<ulong> Completion) : ShardLogMessage;
    sealed record WalRecordCommitted(WalRecord Record) : ShardLogMessage;
}
