using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using Remontoire.Storage;

namespace Remontoire.Messaging;

/// <summary>
/// Per-consumer-group ack state for one shard, kept in memory and rebuilt from the same replay
/// every restart produces — no separate persistence, exactly like a shard log's own MemTable/SST
/// rebuild rebuilds from the same committed-record replay rather than its own snapshot of state.
/// </summary>
public sealed class AckIndex {
    readonly ConcurrentDictionary<string, ConsumerGroupAckState> _groups = new();

    /// <summary>
    /// Applies one Ack record — a no-op for any other <see cref="WalRecordType"/>.
    /// </summary>
    public void Apply(WalRecord record) {
        if (record.RecordType != WalRecordType.Ack)
            return;

        var consumerGroup = Encoding.UTF8.GetString(record.PartitionKey.Span);
        var state = _groups.GetOrAdd(consumerGroup, _ => new ConsumerGroupAckState());
        state.Ack(DecodeAckPayload(record.Payload.Span));
    }

    /// <summary>
    /// Applies offsets directly, bypassing Raft entirely — checkpoint mode's cheap path. Funnels
    /// into <see cref="ConsumerGroupAckState.ApplyLocally"/>, deliberately not
    /// <see cref="ConsumerGroupAckState.Ack"/>: this offset range has not gone through Raft, and
    /// might never — advancing the same committed watermark <see cref="ConsumerGroupAckState.Ack"/>
    /// advances would let an isolated, stale leader's pruning pass act on acks no quorum ever
    /// agreed to. Callable only from the group's own leader (enforced by the caller).
    /// </summary>
    public void ApplyLocal(string consumerGroup, IReadOnlyList<ulong> offsets) {
        var state = _groups.GetOrAdd(consumerGroup, _ => new ConsumerGroupAckState());
        state.ApplyLocally(offsets);
    }

    /// <summary>
    /// The current ack state for <paramref name="consumerGroup"/> — a fresh, all-unacked state
    /// (<see cref="ConsumerGroupAckState.LowWatermark"/> zero) for a group that has never acked
    /// anything yet, so a new consumer group starts correctly at offset 0, not at a missing-key
    /// exception.
    /// </summary>
    public ConsumerGroupAckState GetOrCreate(string consumerGroup) => _groups.GetOrAdd(consumerGroup, _ => new ConsumerGroupAckState());

    /// <summary>
    /// Every consumer group this ack index has ever seen an Ack record for.
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
}
