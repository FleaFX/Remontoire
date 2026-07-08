using System.Text;
using FluentAssertions;

namespace Remontoire.Sharding;

public class ShardRouterTests {
    public class GetVirtualShardIndex {
        // Frozen regression anchors — the actual output of ShardRouter.GetVirtualShardIndex
        // (XxHash3, seed 0) for these inputs, captured once and hard-coded here. There is no
        // externally published test vector for this exact (algorithm, seed) combination, unlike
        // Crc32C's Castagnoli check value — these values are this project's own anchor. If this
        // test ever goes red without an intentional RoutingAlgorithm change, routing for every
        // existing stream has silently shifted (shard-routing-design.md §6/§8).
        [Theory]
        [InlineData("order-12345", 1024, 433)]
        [InlineData("customer-A", 16, 9)]
        [InlineData("hello world", 1024, 139)]
        [InlineData("hello world", 3, 1)]
        [InlineData("a", 2, 1)]
        public void Matches_frozen_known_vector(string partitionKey, int virtualShardCount, int expectedShardIndex) =>
            ShardRouter.GetVirtualShardIndex(Encoding.UTF8.GetBytes(partitionKey), virtualShardCount).Should().Be(expectedShardIndex);

        [Fact]
        public void Is_deterministic_across_repeated_calls() {
            var partitionKey = "repeat-me"u8.ToArray();

            var first = ShardRouter.GetVirtualShardIndex(partitionKey, 1024);
            for (var i = 0; i < 100; i++)
                ShardRouter.GetVirtualShardIndex(partitionKey, 1024).Should().Be(first);
        }

        [Theory]
        [InlineData("")]
        [InlineData("any-key")]
        [InlineData("a-completely-different-key")]
        public void Always_returns_zero_when_there_is_only_one_virtual_shard(string partitionKey) =>
            ShardRouter.GetVirtualShardIndex(Encoding.UTF8.GetBytes(partitionKey), 1).Should().Be(0);

        [Fact]
        public void Empty_partition_key_yields_a_valid_deterministic_result() {
            var first = ShardRouter.GetVirtualShardIndex(ReadOnlySpan<byte>.Empty, 1024);

            first.Should().BeInRange(0, 1023);
            ShardRouter.GetVirtualShardIndex(ReadOnlySpan<byte>.Empty, 1024).Should().Be(first);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(int.MinValue)]
        public void Throws_when_virtual_shard_count_is_not_positive(int virtualShardCount) {
            var act = () => ShardRouter.GetVirtualShardIndex("any-key"u8, virtualShardCount);

            act.Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Throws_when_the_algorithm_is_not_a_known_member() {
            var act = () => ShardRouter.GetVirtualShardIndex("any-key"u8, 1024, (RoutingAlgorithm)255);

            act.Should().Throw<ArgumentOutOfRangeException>();
        }

        // Sanity-checks the application of XxHash3's already-proven distribution quality, not
        // the algorithm itself (shard-routing-design.md §8) — a large, fixed-seed sample of
        // random keys should spread roughly evenly across virtual shards, including for small
        // and prime virtualShardCount values (not only powers of two). Deterministic: Random(42)
        // is fixed, so this never flakes run-to-run.
        [Theory]
        [InlineData(1024)]
        [InlineData(3)]
        [InlineData(17)]
        public void Spreads_random_keys_roughly_evenly_across_virtual_shards(int virtualShardCount) {
            const int sampleSize = 500_000;
            const double allowedDeviation = 0.2;

            var counts = new int[virtualShardCount];
            var random = new Random(42);
            var partitionKey = new byte[16];

            for (var i = 0; i < sampleSize; i++) {
                random.NextBytes(partitionKey);
                counts[ShardRouter.GetVirtualShardIndex(partitionKey, virtualShardCount)]++;
            }

            var expectedPerShard = (double)sampleSize / virtualShardCount;
            counts.Should().OnlyContain(count => Math.Abs(count - expectedPerShard) <= expectedPerShard * allowedDeviation);
        }
    }
}
