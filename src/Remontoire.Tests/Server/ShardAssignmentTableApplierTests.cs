using FluentAssertions;
using Remontoire.Raft;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

// Layer 2: a real, single-node RaftReplica (standing in for the meta-group) feeding a real
// ShardAssignmentTableApplier, proving the tail-and-apply loop actually materializes committed
// admin commands — the same composition shape ClientProtocolCompositionTests already uses one
// layer down for AckIndexApplier.
public class ShardAssignmentTableApplierTests {
    [Fact]
    public async Task A_committed_CreateStream_command_becomes_visible_through_the_table() {
        var (replica, table, applier) = await ComposeAsync();
        try {
            await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream("orders", 1024, RoutingAlgorithm.XxHash3V1))));

            (await WaitUntilAsync(() => table.TryGetStreamConfig("orders", out _))).Should().BeTrue();
            table.TryGetStreamConfig("orders", out var config);
            config.Should().Be(new StreamShardingConfig("orders", 1024, RoutingAlgorithm.XxHash3V1));
        } finally {
            await applier.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_migration_lifecycle_is_replayed_into_the_table_in_commit_order() {
        var (replica, table, applier) = await ComposeAsync();
        try {
            await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new MigrationStarted("migration-1", "orders", 5, "group-1", "group-2"))));
            await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new Cutover("migration-1", "orders", 5, "group-2"))));

            (await WaitUntilAsync(() => table.TryGetAssignment("orders", 5, out var assignment) && assignment.GroupId == "group-2"))
                .Should().BeTrue();
            table.TryGetAssignment("orders", 5, out var final);
            final.MigratingToGroupId.Should().BeNull();
        } finally {
            await applier.DisposeAsync();
        }
    }

    [Fact]
    public async Task Also_feeds_a_MetaLogJournal_when_one_is_given() {
        var (replica, _, applier, journal) = await ComposeWithJournalAsync();
        try {
            await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream("orders", 1024, RoutingAlgorithm.XxHash3V1))));

            (await WaitUntilAsync(() => journal!.Snapshot().Records.Count == 1)).Should().BeTrue();
            var (version, records) = journal!.Snapshot();
            version.Should().Be(records[0].Version);
            MetaLogRecord.Decode(records[0].Payload).Should().Be(new CreateStream("orders", 1024, RoutingAlgorithm.XxHash3V1));
        } finally {
            await applier.DisposeAsync();
        }
    }

    static async Task<(RaftReplica Replica, ShardAssignmentTable Table, ShardAssignmentTableApplier Applier)> ComposeAsync() {
        var (replica, table, applier, _) = await ComposeWithJournalAsync(withJournal: false);
        return (replica, table, applier);
    }

    static async Task<(RaftReplica Replica, ShardAssignmentTable Table, ShardAssignmentTableApplier Applier, MetaLogJournal? Journal)> ComposeWithJournalAsync(bool withJournal = true) {
        var config = new RaftReplicaConfig(
            GroupId: "__meta__", NodeId: "node-1", Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync();
        replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node group -> ready leader
        await replica.DrainAsync();

        var table = new ShardAssignmentTable();
        var journal = withJournal ? new MetaLogJournal() : null;
        var applier = new ShardAssignmentTableApplier(replica, table, journal);

        return (replica, table, applier, journal);
    }

    static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null) {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(2));
        while (DateTime.UtcNow < deadline) {
            if (condition())
                return true;

            await Task.Delay(5);
        }

        return condition();
    }
}
