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

        state.Ack(0);
        state.Ack(1);
        state.Ack(2);

        state.LowWatermark.Should().Be(3, "exclusive — offsets 0, 1, and 2 are acked, 3 is not");
        state.IsAcked(2).Should().BeTrue();
    }

    [Fact]
    public void Ack_tracks_an_out_of_order_offset_selectively_without_moving_the_watermark() {
        var state = new ConsumerGroupAckState();

        state.Ack(5);

        state.LowWatermark.Should().Be(0);
        state.IsAcked(5).Should().BeTrue();
        state.IsAcked(3).Should().BeFalse("only offset 5 was acked, not everything up to it");
    }

    [Fact]
    public void Ack_collapses_the_watermark_once_a_gap_is_filled() {
        var state = new ConsumerGroupAckState();
        state.Ack(0);
        state.Ack(2); // gap at 1 — held selectively

        state.Ack(1); // fills the gap

        state.LowWatermark.Should().Be(3, "exclusive — offsets 0, 1, and 2 are all now acked");
    }

    [Fact]
    public void Ack_is_idempotent_for_an_already_covered_offset() {
        var state = new ConsumerGroupAckState();
        state.Ack(0);
        state.Ack(1);

        state.Ack(0); // already covered by the watermark

        state.LowWatermark.Should().Be(2, "exclusive — offsets 0 and 1 are acked, 2 is not");
    }
}
