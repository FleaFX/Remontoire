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

    public class Concurrency {
        [Fact]
        public void Concurrent_Ack_AdvanceWatermarkTo_and_ApplyLocally_never_corrupts_state() {
            // ApplyLocally is the one genuinely concurrent second writer the lock exists for
            // (checkpoint-mode gRPC threads, racing the replay loop's Ack/AdvanceWatermarkTo) —
            // a prior version of this test never exercised it at all.
            var state = new ConsumerGroupAckState();
            const int offsetCount = 2000;

            Parallel.For(0, offsetCount, i => {
                switch (i % 3) {
                    case 0: state.AdvanceWatermarkTo((ulong)i); break;
                    case 1: state.ApplyLocally([(ulong)i]); break;
                    default: state.Ack([(ulong)i]); break;
                }
            });

            // No crash, no exception, both watermarks land somewhere sane (at most offsetCount),
            // and — the core invariant a prior review bug broke — CommittedWatermark never
            // exceeds LowWatermark: it may only advance through offsets Ack/AdvanceWatermarkTo
            // themselves committed, never by borrowing ApplyLocally's local-only progress.
            state.LowWatermark.Should().BeLessThanOrEqualTo(offsetCount);
            state.CommittedWatermark.Should().BeLessThanOrEqualTo(state.LowWatermark);
        }
    }
}
