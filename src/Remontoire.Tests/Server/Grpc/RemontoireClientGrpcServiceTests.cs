using FluentAssertions;

namespace Remontoire.Server.Grpc;

// Regression coverage for a confirmed review bug: checkpoint-mode Ack applies client-supplied
// offsets straight into AckIndex with no upper bound against the log's real end — cheap to forge
// (no Raft round-trip), unlike the strict path. Tested directly against the extracted, pure
// filtering rule — the RPC method itself needs a real ServerCallContext (no fake precedent in
// this codebase), so its own end-to-end coverage lives in the real gRPC cluster harness instead.
public class RemontoireClientGrpcServiceTests {
    public class WithinLogBounds {
        [Fact]
        public void Keeps_every_offset_below_the_logs_current_end() {
            RemontoireClientGrpcService.WithinLogBounds([0, 1, 2], nextOffsetToApply: 3).Should().Equal(0UL, 1UL, 2UL);
        }

        [Fact]
        public void Drops_offsets_at_or_beyond_the_logs_current_end() {
            RemontoireClientGrpcService.WithinLogBounds([0, 1, 1_000_000], nextOffsetToApply: 2).Should().Equal(0UL, 1UL);
        }

        [Fact]
        public void Returns_empty_when_every_offset_is_out_of_bounds() {
            RemontoireClientGrpcService.WithinLogBounds([5, 6, 7], nextOffsetToApply: 0).Should().BeEmpty();
        }
    }
}
