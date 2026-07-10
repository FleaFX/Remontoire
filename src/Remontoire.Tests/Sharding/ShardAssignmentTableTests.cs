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
        public void A_late_replayed_MigrationStarted_after_its_own_cutover_already_completed_is_rejected() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));
            table.Apply(new Cutover(Migration1, "orders", 5, "group-2"));

            // A stale redelivery of the ORIGINAL MigrationStarted, arriving after its own
            // migration already cut over — must not resurrect the old routing.
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be("group-2", "a completed migration must never be reverted by a late replay of its own start command");
            assignment.MigratingToGroupId.Should().BeNull();
        }

        [Fact]
        public void A_new_migration_for_the_same_shard_is_allowed_after_an_earlier_one_completed() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(Migration1, "orders", 5, "group-1", "group-2"));
            table.Apply(new Cutover(Migration1, "orders", 5, "group-2"));

            table.Apply(new MigrationStarted(Migration2, "orders", 5, "group-2", "group-3"));

            table.TryGetAssignment("orders", 5, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be("group-2", "routing stays on the current group until the new migration's own cutover");
            assignment.MigratingToGroupId.Should().Be("group-3");
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

        [Fact]
        public void SetConsumerGroupAckMode_changes_only_the_mode_not_the_mandatory_flag() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetConsumerGroupMandatory("orders", "billing", false));

            table.Apply(new SetConsumerGroupAckMode("orders", "billing", AckMode.Checkpoint));

            var policy = table.GetConsumerGroupPolicy("orders", "billing");
            policy.Mode.Should().Be(AckMode.Checkpoint);
            policy.Mandatory.Should().BeFalse("SetConsumerGroupAckMode must not revert an earlier, independent SetConsumerGroupMandatory");
        }

        [Fact]
        public void SetConsumerGroupMandatory_changes_only_the_mandatory_flag_not_the_mode() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetConsumerGroupAckMode("orders", "billing", AckMode.Checkpoint));

            table.Apply(new SetConsumerGroupMandatory("orders", "billing", false));

            var policy = table.GetConsumerGroupPolicy("orders", "billing");
            policy.Mode.Should().Be(AckMode.Checkpoint, "SetConsumerGroupMandatory must not revert an earlier, independent SetConsumerGroupAckMode");
            policy.Mandatory.Should().BeFalse();
        }

        [Fact]
        public void SetStreamRetentionPolicy_changes_retention_without_touching_the_checkpoint_interval() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetStreamCheckpointInterval("orders", TimeSpan.FromSeconds(30), 500));

            table.Apply(new SetStreamRetentionPolicy("orders", TimeSpan.FromDays(3), TimeSpan.FromDays(14), MaxSizeBytesPerVirtualShard: 1024));

            var policy = table.GetRetentionPolicy("orders");
            policy.AuditRetention.Should().Be(TimeSpan.FromDays(3));
            policy.MaxRetention.Should().Be(TimeSpan.FromDays(14));
            policy.MaxSizeBytesPerVirtualShard.Should().Be(1024);
            policy.CheckpointInterval.Should().Be(TimeSpan.FromSeconds(30), "SetStreamRetentionPolicy must not revert an earlier, independent SetStreamCheckpointInterval");
            policy.CheckpointOffsetCount.Should().Be(500);
        }

        [Fact]
        public void SetStreamCheckpointInterval_changes_the_checkpoint_interval_without_touching_retention() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetStreamRetentionPolicy("orders", TimeSpan.FromDays(3), TimeSpan.FromDays(14), MaxSizeBytesPerVirtualShard: 1024));

            table.Apply(new SetStreamCheckpointInterval("orders", TimeSpan.FromSeconds(30), 500));

            var policy = table.GetRetentionPolicy("orders");
            policy.CheckpointInterval.Should().Be(TimeSpan.FromSeconds(30));
            policy.CheckpointOffsetCount.Should().Be(500);
            policy.AuditRetention.Should().Be(TimeSpan.FromDays(3), "SetStreamCheckpointInterval must not revert an earlier, independent SetStreamRetentionPolicy");
            policy.MaxRetention.Should().Be(TimeSpan.FromDays(14));
            policy.MaxSizeBytesPerVirtualShard.Should().Be(1024);
        }
    }

    public class GetConsumerGroupPolicy {
        [Fact]
        public void Defaults_to_strict_and_mandatory_for_a_never_touched_group() {
            var table = new ShardAssignmentTable();

            var policy = table.GetConsumerGroupPolicy("orders", "billing");

            policy.Should().Be(new ConsumerGroupPolicy(AckMode.Strict, Mandatory: true));
        }
    }

    public class GetRetentionPolicy {
        [Fact]
        public void Defaults_to_a_seven_and_thirty_day_window_with_no_size_ceiling_for_a_never_touched_stream() {
            var table = new ShardAssignmentTable();

            var policy = table.GetRetentionPolicy("orders");

            policy.Should().Be(new StreamRetentionPolicy(TimeSpan.FromDays(7), TimeSpan.FromDays(30), null, null, null));
        }
    }

    public class EnumerateAssignments {
        [Fact]
        public void Returns_an_empty_collection_when_nothing_is_assigned_yet() {
            var table = new ShardAssignmentTable();

            table.EnumerateAssignments().Should().BeEmpty();
        }

        [Fact]
        public void Returns_every_currently_known_assignment() {
            var table = new ShardAssignmentTable();
            table.Apply(new MigrationStarted(new MigrationId(Guid.NewGuid()), "orders", 5, "group-1", "group-2"));
            table.Apply(new MigrationStarted(new MigrationId(Guid.NewGuid()), "shipments", 0, "group-3", "group-4"));

            var assignments = table.EnumerateAssignments();

            assignments.Should().HaveCount(2);
            assignments.Should().Contain(a => a.StreamName == "orders" && a.GroupId == "group-1");
            assignments.Should().Contain(a => a.StreamName == "shipments" && a.GroupId == "group-3");
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

public class ConsumerGroupPolicyTests {
    [Fact]
    public void Two_policies_with_the_same_field_values_are_equal() {
        var first = new ConsumerGroupPolicy(AckMode.Checkpoint, Mandatory: false);
        var second = new ConsumerGroupPolicy(AckMode.Checkpoint, Mandatory: false);

        first.Should().Be(second);
    }

    [Fact]
    public void Policies_with_a_different_mode_are_not_equal() {
        var first = new ConsumerGroupPolicy(AckMode.Strict, Mandatory: true);
        var second = new ConsumerGroupPolicy(AckMode.Checkpoint, Mandatory: true);

        first.Should().NotBe(second);
    }
}

public class StreamRetentionPolicyTests {
    [Fact]
    public void Two_policies_with_the_same_field_values_are_equal() {
        var first = new StreamRetentionPolicy(TimeSpan.FromDays(7), TimeSpan.FromDays(30), 1024, TimeSpan.FromSeconds(30), 500);
        var second = new StreamRetentionPolicy(TimeSpan.FromDays(7), TimeSpan.FromDays(30), 1024, TimeSpan.FromSeconds(30), 500);

        first.Should().Be(second);
    }

    [Fact]
    public void Policies_with_a_different_size_ceiling_are_not_equal() {
        var first = new StreamRetentionPolicy(TimeSpan.FromDays(7), TimeSpan.FromDays(30), 1024, null, null);
        var second = new StreamRetentionPolicy(TimeSpan.FromDays(7), TimeSpan.FromDays(30), null, null, null);

        first.Should().NotBe(second);
    }
}
