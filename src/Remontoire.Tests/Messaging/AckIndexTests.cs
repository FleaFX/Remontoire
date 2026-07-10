using System.Buffers.Binary;
using System.Text;
using FluentAssertions;
using Remontoire.Storage;

namespace Remontoire.Messaging;

public class AckIndexTests {
    [Fact]
    public void Apply_ignores_a_non_Ack_record() {
        var index = new AckIndex();
        var appendRecord = new WalRecord(WalRecordType.Append, RaftTerm: 0, RaftIndex: 0, LogicalOffset: 0, TimestampMicros: 42,
            "group-1"u8.ToArray(), [], EncodeOffsets(0));

        index.Apply(appendRecord);

        index.GetOrCreate("group-1").LowWatermark.Should().Be(0);
    }

    [Fact]
    public void Apply_acks_the_offsets_carried_by_the_record_for_its_consumer_group() {
        var index = new AckIndex();

        index.Apply(AckRecord("group-1", 0));

        index.GetOrCreate("group-1").IsAcked(0).Should().BeTrue();
    }

    [Fact]
    public void Apply_decodes_a_batch_of_offsets_in_one_record() {
        var index = new AckIndex();

        index.Apply(AckRecord("group-1", 0, 1, 2));

        index.GetOrCreate("group-1").LowWatermark.Should().Be(3, "exclusive — offsets 0, 1, and 2 are acked, 3 is not");
    }

    [Fact]
    public void Apply_keeps_each_consumer_group_independent() {
        var index = new AckIndex();

        index.Apply(AckRecord("group-1", 0));

        index.GetOrCreate("group-2").LowWatermark.Should().Be(0);
        index.GetOrCreate("group-2").IsAcked(0).Should().BeFalse();
    }

    [Fact]
    public void GetOrCreate_returns_a_fresh_all_unacked_state_for_an_unknown_group() {
        var index = new AckIndex();

        var state = index.GetOrCreate("never-seen");

        state.LowWatermark.Should().Be(0);
    }

    [Fact]
    public void AllGroupsLowWatermark_is_zero_when_no_groups_are_registered() {
        var index = new AckIndex();

        index.AllGroupsLowWatermark().Should().Be(0);
    }

    [Fact]
    public void AllGroupsLowWatermark_returns_the_minimum_across_every_registered_group() {
        var index = new AckIndex();
        index.Apply(AckRecord("group-1", 0, 1, 2));
        index.Apply(AckRecord("group-2", 0));

        index.AllGroupsLowWatermark().Should().Be(1, "group-2 is the furthest behind — only offset 0 acked, exclusive watermark 1");
    }

    static WalRecord AckRecord(string consumerGroup, params ulong[] offsets) =>
        new(WalRecordType.Ack, RaftTerm: 0, RaftIndex: 0, LogicalOffset: 0, TimestampMicros: 42,
            Encoding.UTF8.GetBytes(consumerGroup), [], EncodeOffsets(offsets));

    // The wire format an Ack record's payload always carries: [uint32 count][uint64 offsets...], little-endian.
    static byte[] EncodeOffsets(params ulong[] offsets) {
        var buffer = new byte[4 + offsets.Length * 8];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer, (uint)offsets.Length);

        for (var i = 0; i < offsets.Length; i++)
            BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(4 + i * 8, 8), offsets[i]);

        return buffer;
    }
}
