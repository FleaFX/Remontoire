using System.Buffers.Binary;
using System.Text;
using FluentAssertions;
using Remontoire.Storage;

namespace Remontoire.Messaging;

public class AckIndexTests {
    [Fact]
    public async Task Apply_ignores_a_non_Ack_record() {
        var index = new AckIndex();
        var appendRecord = new WalRecord(WalRecordType.Append, RaftTerm: 0, RaftIndex: 0, LogicalOffset: 0, TimestampMicros: 42,
            "group-1"u8.ToArray(), [], EncodeOffsets(0));

        await index.ApplyAsync(appendRecord);

        index.GetOrCreate("group-1").LowWatermark.Should().Be(0);
    }

    [Fact]
    public async Task Apply_acks_the_offsets_carried_by_the_record_for_its_consumer_group() {
        var index = new AckIndex();

        await index.ApplyAsync(AckRecord("group-1", 0));

        index.GetOrCreate("group-1").IsAcked(0).Should().BeTrue();
    }

    [Fact]
    public async Task Apply_decodes_a_batch_of_offsets_in_one_record() {
        var index = new AckIndex();

        await index.ApplyAsync(AckRecord("group-1", 0, 1, 2));

        index.GetOrCreate("group-1").LowWatermark.Should().Be(3, "exclusive — offsets 0, 1, and 2 are acked, 3 is not");
    }

    [Fact]
    public async Task Apply_keeps_each_consumer_group_independent() {
        var index = new AckIndex();

        await index.ApplyAsync(AckRecord("group-1", 0));

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
    public async Task AllGroupsLowWatermark_returns_the_minimum_across_every_registered_group() {
        var index = new AckIndex();
        await index.ApplyAsync(AckRecord("group-1", 0, 1, 2));
        await index.ApplyAsync(AckRecord("group-2", 0));

        index.AllGroupsLowWatermark().Should().Be(1, "group-2 is the furthest behind — only offset 0 acked, exclusive watermark 1");
    }

    public class ApplyLocal {
        [Fact]
        public async Task Applies_a_batch_of_offsets_directly_without_a_WalRecord() {
            var index = new AckIndex();

            await index.ApplyLocalAsync("group-1", [0, 1, 2]);

            index.GetOrCreate("group-1").LowWatermark.Should().Be(3, "exclusive — offsets 0, 1, and 2 are acked, 3 is not");
        }

        [Fact]
        public async Task Keeps_each_consumer_group_independent() {
            var index = new AckIndex();

            await index.ApplyLocalAsync("group-1", [0]);

            index.GetOrCreate("group-2").LowWatermark.Should().Be(0);
        }

        [Fact]
        public async Task Never_advances_CommittedWatermark_only_AdvanceWatermarkTo_does() {
            var index = new AckIndex();

            await index.ApplyLocalAsync("group-1", [0, 1, 2]);

            index.GetOrCreate("group-1").CommittedWatermark.Should().Be(0,
                "no quorum has agreed to this yet — pruning must never act on an ApplyLocal-only offset range");
            index.MandatoryGroupsLowWatermark(_ => true).Should().Be(0, "MandatoryGroupsLowWatermark reads CommittedWatermark, not LowWatermark");
        }
    }

    public class RegisteredConsumerGroups {
        [Fact]
        public void Is_empty_when_no_group_has_ever_acked_anything() {
            var index = new AckIndex();

            index.RegisteredConsumerGroups().Should().BeEmpty();
        }

        [Fact]
        public async Task Returns_every_group_that_has_acked_at_least_once() {
            var index = new AckIndex();
            await index.ApplyAsync(AckRecord("group-1", 0));
            await index.ApplyLocalAsync("group-2", [0]);

            index.RegisteredConsumerGroups().Should().BeEquivalentTo(["group-1", "group-2"]);
        }
    }

    public class MandatoryGroupsLowWatermark {
        [Fact]
        public void Is_zero_when_no_mandatory_group_is_registered() {
            var index = new AckIndex();

            index.MandatoryGroupsLowWatermark(_ => true).Should().Be(0);
        }

        [Fact]
        public async Task Ignores_best_effort_groups_entirely() {
            var index = new AckIndex();
            await index.ApplyAsync(AckRecord("mandatory-group", 0, 1, 2));
            // best-effort-group never acks anything — would otherwise drag the minimum to 0.

            index.MandatoryGroupsLowWatermark(consumerGroup => consumerGroup == "mandatory-group").Should().Be(3);
        }

        [Fact]
        public async Task Returns_the_minimum_across_mandatory_groups_only() {
            var index = new AckIndex();
            await index.ApplyAsync(AckRecord("mandatory-1", 0, 1, 2));
            await index.ApplyAsync(AckRecord("mandatory-2", 0));
            await index.ApplyAsync(AckRecord("best-effort", 0, 1));

            index.MandatoryGroupsLowWatermark(consumerGroup => consumerGroup != "best-effort")
                .Should().Be(1, "mandatory-2 is the furthest-behind mandatory group");
        }
    }

    public class SlowestMandatoryGroup {
        [Fact]
        public void Is_null_when_no_mandatory_group_is_registered() {
            var index = new AckIndex();

            index.SlowestMandatoryGroup(_ => false).Should().BeNull();
        }

        [Fact]
        public async Task Names_the_furthest_behind_mandatory_group() {
            var index = new AckIndex();
            await index.ApplyAsync(AckRecord("mandatory-1", 0, 1, 2));
            await index.ApplyAsync(AckRecord("mandatory-2", 0));
            await index.ApplyAsync(AckRecord("best-effort", 0, 1));

            var slowest = index.SlowestMandatoryGroup(consumerGroup => consumerGroup != "best-effort");

            slowest.Should().NotBeNull();
            slowest!.Value.ConsumerGroup.Should().Be("mandatory-2");
            slowest.Value.CommittedWatermark.Should().Be(1);
        }
    }

    public class Concurrency {
        [Fact]
        public async Task Concurrent_ApplyAsync_and_ApplyLocalAsync_never_corrupt_state() {
            // The real concurrent pair: the replay loop's ApplyAsync racing checkpoint mode's
            // ApplyLocalAsync from multiple gRPC-like callers — all against the same group,
            // serialized entirely by AckIndex's own actor mailbox, never by a lock.
            var index = new AckIndex();
            const int offsetCount = 2000;

            await Task.WhenAll(Enumerable.Range(0, offsetCount).Select(async i => {
                if (i % 3 == 0)
                    await index.ApplyAsync(AckRecord("group-1", (ulong)i));
                else
                    await index.ApplyLocalAsync("group-1", [(ulong)i]);
            }));

            var state = index.GetOrCreate("group-1");
            // No crash, no exception, both watermarks land somewhere sane (at most offsetCount),
            // and — the invariant a prior lock-based design broke — CommittedWatermark never
            // exceeds LowWatermark: it may only advance through offsets ApplyAsync itself
            // committed, never by borrowing ApplyLocalAsync's local-only progress.
            state.LowWatermark.Should().BeLessThanOrEqualTo(offsetCount);
            state.CommittedWatermark.Should().BeLessThanOrEqualTo(state.LowWatermark);
        }
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
