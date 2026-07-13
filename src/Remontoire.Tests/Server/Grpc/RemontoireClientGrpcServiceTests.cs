using System.Diagnostics;
using System.Text;
using FluentAssertions;
using Remontoire.Storage;

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

    // Same "extracted, pure logic tested directly" rationale as WithinLogBounds above — a real
    // Activity/ActivityListener needs no ServerCallContext, so this is testable in full isolation.
    public class LinkToStoredCorrelationContext {
        static readonly ActivitySource TestSource = new("Remontoire.Tests.CorrelationIdAndTracing");

        [Fact]
        public void Adds_a_Link_to_the_stored_correlation_context_never_a_parent() {
            using var listener = new ActivityListener {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            };
            ActivitySource.AddActivityListener(listener);

            using var publishActivity = TestSource.StartActivity("publish")!;
            var storedTraceparent = publishActivity.Id!;
            var headers = new[] {
                new Header(Encoding.UTF8.GetBytes(RemontoireClientGrpcService.CorrelationIdHeaderKey), Encoding.UTF8.GetBytes(storedTraceparent)),
            };
            publishActivity.Stop();

            using var consumeActivity = TestSource.StartActivity("consume")!;
            RemontoireClientGrpcService.LinkToStoredCorrelationContext(headers);

            // Links, not ParentId: LinkToStoredCorrelationContext only ever calls AddLink, never
            // anything that could set ParentId — Activity.ParentId is fixed permanently at
            // construction anyway (before this method is ever called), so asserting on it here
            // would test the BCL's own Activity API, not this method's behavior.
            consumeActivity.Links.Should().ContainSingle(link => link.Context.TraceId == publishActivity.TraceId);
        }

        [Fact]
        public void Is_a_no_op_when_no_correlation_header_is_present() {
            using var listener = new ActivityListener {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            };
            ActivitySource.AddActivityListener(listener);

            using var consumeActivity = TestSource.StartActivity("consume")!;
            RemontoireClientGrpcService.LinkToStoredCorrelationContext([]);

            consumeActivity.Links.Should().BeEmpty();
        }
    }
}
