using FluentAssertions;

namespace Remontoire.Sharding;

public class ShardAssignmentTableTests {
    public class TryGetStreamConfig {
        [Fact]
        public void Returns_false_for_an_unknown_stream() {
            var table = new ShardAssignmentTable();

            table.TryGetStreamConfig("unknown-stream", out var config).Should().BeFalse();
            config.Should().Be(default(StreamShardingConfig));
        }
    }

    public class TryGetGroup {
        [Fact]
        public void Returns_false_for_an_unknown_group() {
            var table = new ShardAssignmentTable();

            table.TryGetGroup("unknown-group", out var group).Should().BeFalse();
            group.Should().Be(default(PhysicalGroupDescriptor));
        }
    }

    public class TryGetAssignment {
        [Fact]
        public void Returns_false_for_an_unknown_stream_and_virtual_shard_index() {
            var table = new ShardAssignmentTable();

            table.TryGetAssignment("unknown-stream", 0, out var assignment).Should().BeFalse();
            assignment.Should().Be(default(VirtualShardAssignment));
        }
    }
}

public class StreamShardingConfigTests {
    [Fact]
    public void Two_configs_with_the_same_field_values_are_equal() {
        var first = new StreamShardingConfig("orders", 1024, RoutingAlgorithm.XxHash3V1);
        var second = new StreamShardingConfig("orders", 1024, RoutingAlgorithm.XxHash3V1);

        first.Should().Be(second);
    }

    [Fact]
    public void Configs_with_a_different_virtual_shard_count_are_not_equal() {
        var first = new StreamShardingConfig("orders", 1024, RoutingAlgorithm.XxHash3V1);
        var second = new StreamShardingConfig("orders", 512, RoutingAlgorithm.XxHash3V1);

        first.Should().NotBe(second);
    }
}

public class PhysicalGroupDescriptorTests {
    [Fact]
    public void Exposes_the_group_id_and_its_members() {
        var members = new[] {
            new ShardGroupMember("node-1", new Uri("https://node-1:5001")),
            new ShardGroupMember("node-2", new Uri("https://node-2:5001")),
        };

        var descriptor = new PhysicalGroupDescriptor("group-1", members);

        descriptor.GroupId.Should().Be("group-1");
        descriptor.Members.Should().BeEquivalentTo(members);
    }
}

public class VirtualShardAssignmentTests {
    [Fact]
    public void MigratingToGroupId_defaults_to_null_when_no_migration_is_in_progress() {
        var assignment = new VirtualShardAssignment("orders", 5, "group-1");

        assignment.MigratingToGroupId.Should().BeNull();
    }

    [Fact]
    public void MigratingToGroupId_holds_the_target_group_during_an_in_progress_reshard() {
        var assignment = new VirtualShardAssignment("orders", 5, "group-1", MigratingToGroupId: "group-2");

        assignment.GroupId.Should().Be("group-1", "routing keeps following the current group until cutover flips it");
        assignment.MigratingToGroupId.Should().Be("group-2");
    }
}
