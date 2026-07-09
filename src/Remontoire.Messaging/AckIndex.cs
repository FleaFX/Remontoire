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

    /// <summary>Applies one Ack record — a no-op for any other <see cref="WalRecordType"/>.</summary>
    public void Apply(WalRecord record) {
        if (record.RecordType != WalRecordType.Ack)
            return;

        var consumerGroup = Encoding.UTF8.GetString(record.PartitionKey.Span);
        var state = _groups.GetOrAdd(consumerGroup, _ => new ConsumerGroupAckState());

        foreach (var offset in DecodeAckPayload(record.Payload.Span))
            state.Ack(offset);
    }

    /// <summary>
    /// The current ack state for <paramref name="consumerGroup"/> — a fresh, all-unacked state
    /// (<see cref="ConsumerGroupAckState.LowWatermark"/> zero) for a group that has never acked
    /// anything yet, so a new consumer group starts correctly at offset 0, not at a missing-key
    /// exception.
    /// </summary>
    public ConsumerGroupAckState GetOrCreate(string consumerGroup) => _groups.GetOrAdd(consumerGroup, _ => new ConsumerGroupAckState());

    /// <summary>
    /// The pruning gate for <see cref="Remontoire.Storage.CompactionPolicy.GetAckedLowWatermarkAsync"/>:
    /// the lowest watermark across every registered consumer group — "every group has acked",
    /// with every group implicitly required (no required/best-effort distinction yet).
    /// </summary>
    public ulong AllGroupsLowWatermark() => _groups.IsEmpty ? 0 : _groups.Values.Min(state => state.LowWatermark);

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
