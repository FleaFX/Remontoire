using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using Remontoire.Storage;

namespace Remontoire.Messaging;

/// <summary>
/// A message posted to <see cref="AckIndex"/>'s actor mailbox. Every mutation of any
/// <see cref="ConsumerGroupAckState"/> this index owns flows through here, processed one at a
/// time in strict arrival order, regardless of which thread posted it — the replay loop and any
/// number of concurrent checkpoint-mode gRPC callers alike. Every message carries its own
/// completion signal, so either caller can await the exact moment its own mutation took effect.
/// </summary>
abstract record AckIndexMessage(TaskCompletionSource Completion);

/// <summary>Posted by <see cref="AckIndex.ApplyAsync"/> — the replay loop's own sequential feed.</summary>
sealed record ApplyCommittedRecord(WalRecord Record, TaskCompletionSource Completion) : AckIndexMessage(Completion);

/// <summary>
/// Posted by <see cref="AckIndex.ApplyLocalAsync"/> — checkpoint mode's cheap path, reachable
/// concurrently from any number of gRPC threads.
/// </summary>
sealed record ApplyLocalRecord(string ConsumerGroup, IReadOnlyList<ulong> Offsets, TaskCompletionSource Completion) : AckIndexMessage(Completion);

/// <summary>
/// Per-consumer-group ack state for one shard, kept in memory and rebuilt from the same replay
/// every restart produces — no separate persistence, exactly like a shard log's own MemTable/SST
/// rebuild rebuilds from the same committed-record replay rather than its own snapshot of state.
/// </summary>
/// <remarks>
/// Owns a single-reader mailbox, the sole writer of every <see cref="ConsumerGroupAckState"/> it
/// holds — the same actor shape <c>ShardLog</c> uses for its own state, adopted here after a
/// confirmed review bug: a plain lock around shared mutable state only prevents corruption, not
/// logical inconsistency between two writers with different guarantees (the replay loop, always
/// Raft-committed, versus checkpoint mode's local, unreplicated <see cref="ApplyLocalAsync"/>).
/// Routing every mutation through one mailbox, one message type per intent, makes that class of
/// bug structurally impossible rather than merely reviewed-and-fixed: <see cref="ApplyLocalRecord"/>'s
/// handler has no way to reach <see cref="ConsumerGroupAckState"/>'s committed-watermark state at
/// all. Reads (<see cref="GetOrCreate"/> and everything built on it) stay lock-free and
/// synchronous — routing every read through the mailbox too would needlessly serialize every
/// retention tick and every <c>Consume</c> call behind it.
/// </remarks>
public sealed class AckIndex : IAsyncDisposable {
    readonly ConcurrentDictionary<string, ConsumerGroupAckState> _groups = new();
    readonly Channel<AckIndexMessage> _mailbox = Channel.CreateUnbounded<AckIndexMessage>(new UnboundedChannelOptions { SingleReader = true });
    readonly CancellationTokenSource _cts = new();
    readonly Task _loop;

    /// <summary>
    /// Starts the actor loop immediately.
    /// </summary>
    public AckIndex() => _loop = Task.Run(RunAsync);

    /// <summary>
    /// Applies one Ack or AckCheckpoint record — a no-op for any other <see cref="WalRecordType"/>.
    /// Awaits the actor's own completion signal before returning, so a caller that processes its
    /// source strictly in order (<see cref="AckIndexApplier"/>'s own replay loop) can rely on this
    /// record's effect being visible before it moves on to the next one.
    /// </summary>
    public async Task ApplyAsync(WalRecord record, CancellationToken cancellationToken = default) {
        if (record.RecordType is not (WalRecordType.Ack or WalRecordType.AckCheckpoint))
            return;

        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _mailbox.Writer.TryWrite(new ApplyCommittedRecord(record, completion));
        await completion.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Applies offsets directly, bypassing Raft entirely — checkpoint mode's cheap path. Funnels
    /// into <see cref="ConsumerGroupAckState.ApplyLocally"/>, deliberately not
    /// <see cref="ConsumerGroupAckState.Ack"/>: this offset range has not gone through Raft, and
    /// might never — advancing the same committed watermark <see cref="ConsumerGroupAckState.Ack"/>
    /// advances would let an isolated, stale leader's pruning pass act on acks no quorum ever
    /// agreed to. Callable only from the group's own leader (enforced by the caller). Awaits the
    /// actor's own completion signal before returning — the caller (an <c>Ack</c> RPC) must not
    /// report success until this is actually visible.
    /// </summary>
    public async Task ApplyLocalAsync(string consumerGroup, IReadOnlyList<ulong> offsets, CancellationToken cancellationToken = default) {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _mailbox.Writer.TryWrite(new ApplyLocalRecord(consumerGroup, offsets, completion));
        await completion.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// The current ack state for <paramref name="consumerGroup"/> — a fresh, all-unacked state
    /// (<see cref="ConsumerGroupAckState.LowWatermark"/> zero) for a group that has never acked
    /// anything yet, so a new consumer group starts correctly at offset 0, not at a missing-key
    /// exception. Lock-free, reachable from any thread — only this type's own actor thread ever
    /// mutates the state a lookup here returns.
    /// </summary>
    public ConsumerGroupAckState GetOrCreate(string consumerGroup) => _groups.GetOrAdd(consumerGroup, _ => new ConsumerGroupAckState());

    /// <summary>
    /// Every consumer group this ack index has ever seen an Ack/AckCheckpoint record, or a local
    /// <see cref="ApplyLocalAsync"/> call, for.
    /// </summary>
    public IReadOnlyCollection<string> RegisteredConsumerGroups() => _groups.Keys.ToArray();

    /// <summary>
    /// The pruning gate for <see cref="Remontoire.Storage.CompactionPolicy.GetAckedLowWatermarkAsync"/>:
    /// the lowest watermark across every registered consumer group — "every group has acked",
    /// with every group implicitly required (no required/best-effort distinction yet).
    /// </summary>
    public ulong AllGroupsLowWatermark() => _groups.IsEmpty ? 0 : _groups.Values.Min(state => state.LowWatermark);

    /// <summary>
    /// The pruning gate for mandatory groups only — replaces <see cref="AllGroupsLowWatermark"/>'s
    /// "every group" semantics wherever a mandatory/best-effort distinction now applies. Best-effort
    /// groups (<paramref name="isMandatory"/> returns <see langword="false"/>) never lower this
    /// watermark, so a stuck best-effort group can never block pruning. Takes the mandatory-ness
    /// check as a delegate rather than a stored flag: this project carries no reference to
    /// <c>Remontoire.Sharding</c>, where that policy lives. Deliberately reads
    /// <see cref="ConsumerGroupAckState.CommittedWatermark"/>, never <see cref="ConsumerGroupAckState.LowWatermark"/>
    /// — pruning may only ever act on what a quorum actually agreed to, never on a checkpoint-mode
    /// group's locally-applied-but-unreplicated progress (see that property's own remarks).
    /// </summary>
    public ulong MandatoryGroupsLowWatermark(Func<string, bool> isMandatory) {
        var mandatoryWatermarks = _groups.Where(pair => isMandatory(pair.Key)).Select(pair => pair.Value.CommittedWatermark).ToArray();
        return mandatoryWatermarks.Length == 0 ? 0 : mandatoryWatermarks.Min();
    }

    /// <summary>
    /// Which mandatory consumer group is currently furthest behind, and its committed watermark —
    /// the group <see cref="MandatoryGroupsLowWatermark"/>'s minimum came from, surfaced by name
    /// for the <c>remontoire_pruning_blocked_by_group</c> metric. <see langword="null"/> when no
    /// mandatory group is registered.
    /// </summary>
    public (string ConsumerGroup, ulong CommittedWatermark)? SlowestMandatoryGroup(Func<string, bool> isMandatory) {
        var mandatory = _groups.Where(pair => isMandatory(pair.Key)).ToArray();
        if (mandatory.Length == 0)
            return null;

        var slowest = mandatory.MinBy(pair => pair.Value.CommittedWatermark);
        return (slowest.Key, slowest.Value.CommittedWatermark);
    }

    // The single-threaded owner of every ConsumerGroupAckState this index holds — the same
    // single-reader-mailbox shape as ShardLog's own actor loop.
    async Task RunAsync() {
        try {
            await foreach (var message in _mailbox.Reader.ReadAllAsync(_cts.Token)) {
                switch (message) {
                    case ApplyCommittedRecord(var record, var completion):
                        ApplyCommitted(record);
                        completion.TrySetResult();
                        break;

                    case ApplyLocalRecord(var consumerGroup, var offsets, var completion):
                        GetOrCreate(consumerGroup).ApplyLocally(offsets);
                        completion.TrySetResult();
                        break;
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
        }

        // No outstanding ApplyAsync/ApplyLocalAsync call may block its caller forever when this
        // index shuts down mid-flight — the mailbox may still hold messages that never got a turn.
        _mailbox.Writer.TryComplete();
        while (_mailbox.Reader.TryRead(out var leftover))
            leftover.Completion.TrySetCanceled();
    }

    void ApplyCommitted(WalRecord record) {
        switch (record.RecordType) {
            case WalRecordType.Ack: {
                var consumerGroup = Encoding.UTF8.GetString(record.PartitionKey.Span);
                GetOrCreate(consumerGroup).Ack(DecodeAckPayload(record.Payload.Span));
                break;
            }
            case WalRecordType.AckCheckpoint: {
                var consumerGroup = Encoding.UTF8.GetString(record.PartitionKey.Span);
                GetOrCreate(consumerGroup).AdvanceWatermarkTo(BinaryPrimitives.ReadUInt64LittleEndian(record.Payload.Span));
                break;
            }
        }
    }

    // The wire format an Ack record's payload always carries: [uint32 count][uint64 offsets...],
    // little-endian. Deliberately a small, independent decoder rather than a shared codec type
    // reached across a project boundary for a handful of bytes.
    static IEnumerable<ulong> DecodeAckPayload(ReadOnlySpan<byte> payload) {
        var count = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        var offsets = new ulong[count];

        for (var i = 0; i < count; i++)
            offsets[i] = BinaryPrimitives.ReadUInt64LittleEndian(payload[(4 + i * 8)..]);

        return offsets;
    }

    /// <summary>
    /// Stops the actor loop and awaits its shutdown. Callers must ensure nothing still posts to
    /// <see cref="ApplyAsync"/>/<see cref="ApplyLocalAsync"/> after this returns.
    /// </summary>
    public async ValueTask DisposeAsync() {
        await _cts.CancelAsync();
        await _loop;
        _cts.Dispose();
    }
}
