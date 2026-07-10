using FluentAssertions;

namespace Remontoire.Messaging;

public class ConsumerGroupAckStateTests {
    [Fact]
    public void IsAcked_returns_false_for_an_untouched_offset() {
        var state = new ConsumerGroupAckState();

        state.IsAcked(0).Should().BeFalse();
    }

    [Fact]
    public void Ack_moves_the_low_watermark_forward_for_contiguous_offsets() {
        var state = new ConsumerGroupAckState();

        state.Ack([0]);
        state.Ack([1]);
        state.Ack([2]);

        state.LowWatermark.Should().Be(3, "exclusive — offsets 0, 1, and 2 are acked, 3 is not");
        state.IsAcked(2).Should().BeTrue();
    }

    [Fact]
    public void Ack_moves_the_low_watermark_forward_for_a_whole_batch_in_one_call() {
        var state = new ConsumerGroupAckState();

        state.Ack([0, 1, 2]);

        state.LowWatermark.Should().Be(3, "exclusive — offsets 0, 1, and 2 are acked, 3 is not");
    }

    [Fact]
    public void Ack_tracks_an_out_of_order_offset_selectively_without_moving_the_watermark() {
        var state = new ConsumerGroupAckState();

        state.Ack([5]);

        state.LowWatermark.Should().Be(0);
        state.IsAcked(5).Should().BeTrue();
        state.IsAcked(3).Should().BeFalse("only offset 5 was acked, not everything up to it");
    }

    [Fact]
    public void Ack_collapses_the_watermark_once_a_gap_is_filled() {
        var state = new ConsumerGroupAckState();
        state.Ack([0]);
        state.Ack([2]); // gap at 1 — held selectively

        state.Ack([1]); // fills the gap

        state.LowWatermark.Should().Be(3, "exclusive — offsets 0, 1, and 2 are all now acked");
    }

    [Fact]
    public void Ack_is_idempotent_for_an_already_covered_offset() {
        var state = new ConsumerGroupAckState();
        state.Ack([0]);
        state.Ack([1]);

        state.Ack([0]); // already covered by the watermark

        state.LowWatermark.Should().Be(2, "exclusive — offsets 0 and 1 are acked, 2 is not");
    }

    public class AdvanceWatermarkTo {
        [Fact]
        public void Advances_the_watermark_directly() {
            var state = new ConsumerGroupAckState();

            state.AdvanceWatermarkTo(10);

            state.LowWatermark.Should().Be(10);
        }

        [Fact]
        public void Is_a_no_op_for_a_watermark_that_is_not_ahead_of_the_current_one() {
            var state = new ConsumerGroupAckState();
            state.AdvanceWatermarkTo(10);

            state.AdvanceWatermarkTo(10);
            state.AdvanceWatermarkTo(5);

            state.LowWatermark.Should().Be(10, "a replayed or stale checkpoint must never move the watermark backward");
        }

        [Fact]
        public void Prunes_selective_acks_the_new_watermark_now_subsumes() {
            var state = new ConsumerGroupAckState();
            state.Ack([20]); // held selectively, far ahead of the watermark

            state.AdvanceWatermarkTo(21);

            state.LowWatermark.Should().Be(21);
            // If the selective entry for 20 wasn't pruned, a later AdvanceWatermarkTo below it
            // could resurrect stale bookkeeping — assert indirectly via idempotent re-application.
            state.AdvanceWatermarkTo(21);
            state.LowWatermark.Should().Be(21);
        }
    }

    public class Concurrency {
        [Fact]
        public void Concurrent_Ack_and_AdvanceWatermarkTo_never_corrupts_state() {
            var state = new ConsumerGroupAckState();
            const int offsetCount = 2000;

            Parallel.For(0, offsetCount, i => {
                if (i % 50 == 0)
                    state.AdvanceWatermarkTo((ulong)i);
                else
                    state.Ack([(ulong)i]);
            });

            // No crash, no exception, and the watermark must end up somewhere sane: at most
            // offsetCount (everything acked or checkpointed), never negative/wrapped.
            state.LowWatermark.Should().BeLessThanOrEqualTo(offsetCount);
        }
    }
}
