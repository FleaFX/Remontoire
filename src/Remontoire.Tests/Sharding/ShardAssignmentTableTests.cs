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

    public class Apply {
        static readonly MigrationId Migration1 = new(Guid.NewGuid());
        static readonly MigrationId Migration2 = new(Guid.NewGuid());

        [Fact]
        public void CreateStream_registers_the_streams_sharding_config() {
            var table = new ShardAssignmentTable();

            table.Apply(new CreateStream("orders", 1024, RoutingAlgorithm.XxHash3V1));

            table.TryGetStreamConfig("orders", out var config).Should().BeTrue();
            config.Should().Be(new StreamShardingConfig("orders", 1024, RoutingAlgorithm.XxHash3V1));
        }

        [Fact]
        public void RegisterGroup_registers_the_groups_membership() {
            var table = new ShardAssignmentTable();
            var members = new[] { new ShardGroupMember("node-1", new Uri("https://node-1:5001")) };

            table.Apply(new RegisterGroup("group-1", members));

            table.TryGetGroup("group-1", out var group).Should().BeTrue();
            group.GroupId.Should().Be("group-1");
            group.Members.Should().BeEquivalentTo(members);
        }

        [Fact]
        public void MigrationStarted_creates_an_assignment_pointing_at_the_from_group_with_a_migration_target() {
            var table = new ShardAssignmentTable();

            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be("group-1", "routing stays on the old group until cutover");
            assignment.MigratingToGroupId.Should().Be("group-2");
        }

        [Fact]
        public void MigrationAborted_clears_the_migration_target_without_changing_routing() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.Apply(new MigrationAborted(Migration1, "orders", 5));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be("group-1");
            assignment.MigratingToGroupId.Should().BeNull();
        }

        [Fact]
        public void MigrationAborted_is_a_no_op_when_no_assignment_exists_yet() {
            var table = new ShardAssignmentTable();

            table.Apply(new MigrationAborted(Migration1, "orders", 5));

            table.TryGetAssignment("orders", 5, out _).Should().BeFalse();
        }

        [Fact]
        public void Cutover_flips_routing_to_the_new_group_and_clears_the_migration_target() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.Apply(new Cutover(Migration1, "orders", 5, "group-2"));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be("group-2");
            assignment.MigratingToGroupId.Should().BeNull();
        }

        [Fact]
        public void MigrationCompleted_leaves_the_assignment_untouched() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));
            table.Apply(new Cutover(Migration1, "orders", 5, "group-2"));

            table.Apply(new MigrationCompleted(Migration1, "orders", 5));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be("group-2");
        }

        [Fact]
        public void Duplicate_migration_started_with_same_MigrationId_is_a_no_op() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be("group-1");
            assignment.MigratingToGroupId.Should().Be("group-2");
        }

        [Fact]
        public void MigrationStarted_with_a_different_MigrationId_is_rejected_while_another_migration_is_in_progress() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.Apply(new MigrationStarted(Migration2, "orders", 5, "group-1", "group-3"));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.MigratingToGroupId.Should().Be("group-2", "the second, conflicting migration must not overwrite the one already in progress");
        }

        [Fact]
        public void MigrationAborted_with_a_mismatched_MigrationId_is_rejected() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.Apply(new MigrationAborted(Migration2, "orders", 5));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.MigratingToGroupId.Should().Be("group-2", "a stale/foreign abort must not cancel a different, still-in-progress migration");
        }

        [Fact]
        public void Cutover_with_a_stale_MigrationId_is_rejected() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.Apply(new Cutover(Migration2, "orders", 5, "group-2"));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be("group-1", "a stale/foreign cutover must never flip routing for someone else's migration");
            assignment.MigratingToGroupId.Should().Be("group-2");
        }

        [Fact]
        public void Cutover_with_no_migration_in_progress_is_rejected() {
            var table = new ShardAssignmentTable();

            table.Apply(new Cutover(Migration1, "orders", 5, "group-2"));

            table.TryGetAssignment("orders", 5, out _).Should().BeFalse();
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
