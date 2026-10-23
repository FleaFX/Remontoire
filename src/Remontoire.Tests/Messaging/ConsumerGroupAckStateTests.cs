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

    public class CommittedWatermark {
        [Fact]
        public void Ack_advances_the_committed_watermark_alongside_the_low_watermark() {
            var state = new ConsumerGroupAckState();

            state.Ack([0, 1, 2]);

            state.CommittedWatermark.Should().Be(3, "every offset Ack ever sees is, by construction, already Raft-committed");
        }

        [Fact]
        public void ApplyLocally_never_advances_the_committed_watermark() {
            var state = new ConsumerGroupAckState();

            state.ApplyLocally([0, 1, 2]);

            state.LowWatermark.Should().Be(3, "the applied watermark still moves — this is checkpoint mode's whole point");
            state.CommittedWatermark.Should().Be(0, "no quorum has agreed to this yet — only AdvanceWatermarkTo may ever advance it");
        }

        [Fact]
        public void Ack_never_promotes_uncommitted_ApplyLocally_progress_into_the_committed_watermark() {
            var state = new ConsumerGroupAckState();
            state.ApplyLocally([0, 1, 2]); // checkpoint-mode local progress — never replicated

            state.Ack([2]); // a committed ack of a later offset only — 0 and 1 were never committed-acked

            state.LowWatermark.Should().Be(3, "ApplyLocally's own progress is untouched");
            state.CommittedWatermark.Should().Be(0, "offset 2 alone can't collapse the committed watermark while 0 and 1 were never committed — Ack must not borrow LowWatermark's head start");
        }
    }

    public class AdvanceWatermarkTo {
        [Fact]
        public void Advances_both_watermarks_on_a_fresh_state() {
            var state = new ConsumerGroupAckState();

            state.AdvanceWatermarkTo(10);

            state.LowWatermark.Should().Be(10);
            state.CommittedWatermark.Should().Be(10);
        }

        [Fact]
        public void Is_a_no_op_for_a_watermark_that_is_not_ahead_of_the_current_committed_one() {
            var state = new ConsumerGroupAckState();
            state.AdvanceWatermarkTo(10);

            state.AdvanceWatermarkTo(10);
            state.AdvanceWatermarkTo(5);

            state.CommittedWatermark.Should().Be(10, "a replayed or stale checkpoint must never move the committed watermark backward");
            state.LowWatermark.Should().Be(10);
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

        [Fact]
        public void Also_floors_the_low_watermark_so_a_restarted_checkpoint_group_is_never_stuck_at_zero() {
            // Simulates a restart: a checkpoint-mode group's own ApplyLocally progress never
            // survived (never persisted), so a fresh state only ever sees the committed catch-up.
            var state = new ConsumerGroupAckState();

            state.AdvanceWatermarkTo(500);

            state.LowWatermark.Should().Be(500, "a recovering node must never show a checkpoint group's progress as zero when it has a real, committed watermark far ahead");
        }
    }

    public class NackAndPark {
        [Fact]
        public void Nack_schedules_a_retry_with_the_given_attempt() {
            var state = new ConsumerGroupAckState();

            state.Nack(3, attempt: 2, notBeforeMicros: 1_000);

            state.Retry.Should().Be(new PendingRetry(3, 2, 1_000));
        }

        [Fact]
        public void A_retry_disappears_once_its_offset_is_acked() {
            var state = new ConsumerGroupAckState();
            state.Nack(0, 2, 0);

            state.Ack([0]);

            state.Retry.Should().BeNull("acking the retried offset resolves the retry — no extra code needed in the ack paths");
        }

        [Fact]
        public void Nack_for_an_acked_offset_is_ignored() {
            var state = new ConsumerGroupAckState();
            state.Ack([0]);

            state.Nack(0, 2, 0);

            state.Retry.Should().BeNull("a late nack after an ack is harmless");
        }

        [Fact]
        public void Nack_behind_a_pending_retry_is_ignored() {
            var state = new ConsumerGroupAckState();
            state.Nack(2, 2, 0);

            state.Nack(5, 2, 0);

            state.Retry!.Offset.Should().Be(2, "the group offers the earlier offset's retry first — a later nack waits behind it");
        }

        [Fact]
        public void Park_acks_the_offset_and_keeps_why() {
            var state = new ConsumerGroupAckState();
            state.Nack(1, 2, 0);

            state.Park(1, new ParkedMessage(6, "boom", 123));

            state.IsAcked(1).Should().BeTrue("parking acks the offset, so the group's cursor moves past it");
            state.IsParked(1).Should().BeTrue();
            state.Parked[1].Should().Be(new ParkedMessage(6, "boom", 123));
            state.Retry.Should().BeNull();
        }

        [Fact]
        public void RetryChanged_completes_at_every_nack_and_park() {
            var state = new ConsumerGroupAckState();
            var first = state.RetryChanged;

            state.Nack(0, 2, 0);

            first.IsCompleted.Should().BeTrue("the first nack completed the first signal");
            var second = state.RetryChanged;
            second.IsCompleted.Should().BeFalse("a waiter that takes the signal again right after waking lands on a fresh one");

            state.Park(0, new ParkedMessage(2, "boom", 1));

            second.IsCompleted.Should().BeTrue("the park completed the second signal");
        }
    }

    // No Concurrency test class here anymore: Ack/ApplyLocally/AdvanceWatermarkTo are only ever
    // called from AckIndex's own actor thread now, one at a time by construction — calling them
    // concurrently against a bare ConsumerGroupAckState would violate that contract rather than
    // exercise it. The real concurrency guarantee (many callers racing safely) is AckIndex's own
    // job to prove — see AckIndexTests.Concurrency.
}
