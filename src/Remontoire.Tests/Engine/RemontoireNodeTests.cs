using FluentAssertions;
using Remontoire.Sharding;

namespace Remontoire.Engine;

public class RemontoireNodeTests {
    // Regression coverage for a confirmed review bug: once a dead-letter stream is provisioned
    // onto the same physical group as its own source stream (§4.4's own v1 convention),
    // ResolveStreamNameForGroup's reverse-scan finds two candidates for that group. A plain
    // FirstOrDefault over ConcurrentDictionary's unordered enumeration could resolve either one —
    // tested directly against the extracted tie-break rule, independent of the table's actual
    // (non-deterministic) iteration order.
    public class PickPrimaryStreamName {
        [Fact]
        public void Prefers_the_real_stream_when_the_dead_letter_shadow_is_enumerated_first() {
            var candidates = new[] {
                new VirtualShardAssignment("orders.__deadletter__", 0, "group-1"),
                new VirtualShardAssignment("orders", 0, "group-1"),
            };

            RemontoireNode.PickPrimaryStreamName(candidates).Should().Be("orders");
        }

        [Fact]
        public void Prefers_the_real_stream_when_the_dead_letter_shadow_is_enumerated_last() {
            var candidates = new[] {
                new VirtualShardAssignment("orders", 0, "group-1"),
                new VirtualShardAssignment("orders.__deadletter__", 0, "group-1"),
            };

            RemontoireNode.PickPrimaryStreamName(candidates).Should().Be("orders");
        }

        [Fact]
        public void Falls_back_to_the_dead_letter_stream_when_it_is_the_only_candidate() {
            var candidates = new[] { new VirtualShardAssignment("orders.__deadletter__", 0, "group-1") };

            RemontoireNode.PickPrimaryStreamName(candidates).Should().Be("orders.__deadletter__");
        }

        [Fact]
        public void Returns_null_for_no_candidates() {
            RemontoireNode.PickPrimaryStreamName([]).Should().BeNull();
        }
    }
}
