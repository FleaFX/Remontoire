using FluentAssertions;
using Remontoire.Sharding;

namespace Remontoire.Engine;

public class StreamBootstrapTests {
    [Fact]
    public void Records_lists_the_stream_its_groups_and_a_migration_with_cutover_per_shard() {
        var group = new RegisterGroup("g1", []);
        var id0 = StreamBootstrap.DeriveBootstrapMigrationId("orders", 0);
        var id1 = StreamBootstrap.DeriveBootstrapMigrationId("orders", 1);

        var records = StreamBootstrap.Records("orders", 2, RoutingAlgorithm.XxHash3V1, [group], [(0, "g1"), (1, "g1")]).ToList();

        records.Should().Equal(
            new CreateStream("orders", 2, RoutingAlgorithm.XxHash3V1),
            group,
            new MigrationStarted(id0, "orders", 0, "g1", "g1"),
            new Cutover(id0, "orders", 0, "g1"),
            new MigrationStarted(id1, "orders", 1, "g1", "g1"),
            new Cutover(id1, "orders", 1, "g1"));
    }

    public class DeriveBootstrapMigrationId {
        [Fact]
        public void Is_deterministic_for_the_same_stream_and_virtual_shard() {
            var first = StreamBootstrap.DeriveBootstrapMigrationId("orders", 3);
            var second = StreamBootstrap.DeriveBootstrapMigrationId("orders", 3);

            second.Should().Be(first);
        }

        [Fact]
        public void Differs_for_a_different_virtual_shard_index() {
            var first = StreamBootstrap.DeriveBootstrapMigrationId("orders", 3);
            var second = StreamBootstrap.DeriveBootstrapMigrationId("orders", 4);

            second.Should().NotBe(first);
        }

        [Fact]
        public void Differs_for_a_different_stream_name() {
            var first = StreamBootstrap.DeriveBootstrapMigrationId("orders", 3);
            var second = StreamBootstrap.DeriveBootstrapMigrationId("billing", 3);

            second.Should().NotBe(first);
        }
    }
}
