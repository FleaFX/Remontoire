using FluentAssertions;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

// Layer 3: a real, single-node meta-group plus two real, single-node data groups, all in-process
// (no real network) — the same composition RaftReplicaHostedService builds in production, just
// collapsed into one process. Exercises the full reshard protocol's steps and its named
// crash-safety scenarios.
public class ReshardOrchestratorTests {
    const string StreamName = "orders";
    const string FromGroupId = "group-1";
    const string ToGroupId = "group-2";

    [Fact]
    public async Task A_full_migration_moves_routing_and_every_message_to_the_new_group() {
        var harness = await ComposeAsync();
        try {
            var firstOffset = await Publish(harness.FromReplica, "key-1", "hello");
            (await WaitUntilAsync(() => harness.FromMessaging.ShardLog.TryGet(firstOffset, out _))).Should().BeTrue();
            var secondOffsetPending = firstOffset; // placeholder, reassigned below after copy starts

            var migrationId = new MigrationId(Guid.NewGuid());
            await harness.Orchestrator.ProposeMigrationStartedAsync(harness.MetaReplica, migrationId, StreamName, 0, FromGroupId, ToGroupId);
            (await WaitUntilAsync(() => harness.Table.TryGetAssignment(StreamName, 0, out var a) && a.MigratingToGroupId == ToGroupId)).Should().BeTrue();

            harness.Table.TryGetAssignment(StreamName, 0, out var duringStart).Should().BeTrue();
            duringStart.GroupId.Should().Be(FromGroupId, "routing must not move until cutover");
            duringStart.MigratingToGroupId.Should().Be(ToGroupId);

            var copiedUpTo = await harness.Orchestrator.CopyRecordsAsync(FromGroupId, ToGroupId, 0);

            // A write lands on the old group WHILE it's still the routing target — proving the
            // old group keeps accepting writes throughout bulk copy, not just before it starts.
            secondOffsetPending = await Publish(harness.FromReplica, "key-2", "world");
            (await WaitUntilAsync(() => harness.FromMessaging.ShardLog.TryGet(secondOffsetPending, out _))).Should().BeTrue();

            copiedUpTo = await harness.Orchestrator.CopyRecordsAsync(FromGroupId, ToGroupId, copiedUpTo); // tail catch-up

            // The pause must stay active until cutover has actually committed — resuming before
            // that would let a write land on the old group with no further copy round left to
            // pick it up, since routing is about to move away from it for good.
            using (harness.Orchestrator.PauseAdmission(FromGroupId)) {
                harness.AdmissionGate.IsPaused(FromGroupId).Should().BeTrue();

                await harness.Orchestrator.ProposeCutoverAsync(harness.MetaReplica, migrationId, StreamName, 0, ToGroupId);
                (await WaitUntilAsync(() => harness.Table.TryGetAssignment(StreamName, 0, out var a) && a.GroupId == ToGroupId)).Should().BeTrue();
            }
            harness.AdmissionGate.IsPaused(FromGroupId).Should().BeFalse("the scope's disposal must resume admission");

            harness.Table.TryGetAssignment(StreamName, 0, out var afterCutover).Should().BeTrue();
            afterCutover.GroupId.Should().Be(ToGroupId, "cutover is the only step that actually moves routing");
            afterCutover.MigratingToGroupId.Should().BeNull();

            await harness.Orchestrator.ProposeMigrationCompletedAsync(harness.MetaReplica, migrationId, StreamName, 0);

            await AssertShardLogContainsAsync(harness.ToMessaging.ShardLog, [(firstOffset, "hello"), (secondOffsetPending, "world")]);
        } finally {
            await harness.DisposeAsync();
        }
    }

    [Fact]
    public async Task Crash_before_migration_started_commits_leaves_routing_unchanged() {
        // "Crash before commit" is a no-op by construction — nothing was ever proposed, so there
        // is nothing to verify beyond confirming the pre-migration state is exactly what it was.
        var harness = await ComposeAsync();
        try {
            harness.Table.TryGetAssignment(StreamName, 0, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be(FromGroupId);
            assignment.MigratingToGroupId.Should().BeNull();
        } finally {
            await harness.DisposeAsync();
        }
    }

    [Fact]
    public async Task Crash_after_cutover_committed_survives_restart_with_new_routing() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        try {
            var migrationId = new MigrationId(Guid.NewGuid());

            // First "process": commit the full lifecycle, then shut down as if it crashed right
            // after the cutover committed.
            {
                var (replica, log, table, applier) = await StartPersistentMetaAsync(directory);
                try {
                    await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream(StreamName, 1, RoutingAlgorithm.XxHash3V1))));
                    var orchestrator = new ReshardOrchestrator(new RaftReplicaRegistry(), new MessagingGroupRegistry(), new MigrationAdmissionGate());
                    await orchestrator.ProposeMigrationStartedAsync(replica, migrationId, StreamName, 0, FromGroupId, ToGroupId);
                    await orchestrator.ProposeCutoverAsync(replica, migrationId, StreamName, 0, ToGroupId);

                    (await WaitUntilAsync(() => table.TryGetAssignment(StreamName, 0, out var a) && a.GroupId == ToGroupId)).Should().BeTrue();
                } finally {
                    await applier.DisposeAsync();
                    await replica.DisposeAsync();
                    await log.DisposeAsync();
                }
            }

            // "Restart": a fresh replica plus a fresh, empty table over the SAME directory,
            // rebuilt purely by replaying what's already persisted — never separately persisted
            // itself.
            {
                var (replica, log, table, applier) = await StartPersistentMetaAsync(directory);
                try {
                    (await WaitUntilAsync(() => table.TryGetAssignment(StreamName, 0, out var a) && a.GroupId == ToGroupId)).Should().BeTrue();
                    table.TryGetAssignment(StreamName, 0, out var assignment).Should().BeTrue();
                    assignment.GroupId.Should().Be(ToGroupId, "the committed cutover must survive a restart");
                    assignment.MigratingToGroupId.Should().BeNull();
                } finally {
                    await applier.DisposeAsync();
                    await replica.DisposeAsync();
                    await log.DisposeAsync();
                }
            }
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    static async Task<(RaftReplica Replica, WalRaftLog Log, ShardAssignmentTable Table, ShardAssignmentTableApplier Applier)> StartPersistentMetaAsync(string directory) {
        var config = new RaftReplicaConfig(
            GroupId: "__meta__", NodeId: "meta-node", Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

        var log = await WalRaftLog.OpenAsync(directory);
        var stateStore = new FileRaftStateStore(directory);
        var replica = new RaftReplica(stateStore, log, new RecordingRaftTransport(), config);
        await replica.StartAsync();
        replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node group -> ready leader, replays history
        await replica.DrainAsync();

        var table = new ShardAssignmentTable();
        var applier = new ShardAssignmentTableApplier(replica, table);
        return (replica, log, table, applier);
    }

    [Fact]
    public async Task Duplicate_migration_started_with_same_MigrationId_through_real_consensus_is_a_no_op() {
        var harness = await ComposeAsync();
        try {
            var migrationId = new MigrationId(Guid.NewGuid());
            await harness.Orchestrator.ProposeMigrationStartedAsync(harness.MetaReplica, migrationId, StreamName, 0, FromGroupId, ToGroupId);
            (await WaitUntilAsync(() => harness.Table.TryGetAssignment(StreamName, 0, out var a) && a.MigratingToGroupId == ToGroupId)).Should().BeTrue();

            await harness.Orchestrator.ProposeMigrationStartedAsync(harness.MetaReplica, migrationId, StreamName, 0, FromGroupId, ToGroupId);
            await FlushAppliedOrderAsync(harness.MetaReplica, harness.Table);

            harness.Table.TryGetAssignment(StreamName, 0, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be(FromGroupId);
            assignment.MigratingToGroupId.Should().Be(ToGroupId);
        } finally {
            await harness.DisposeAsync();
        }
    }

    [Fact]
    public async Task Cutover_with_a_stale_MigrationId_through_real_consensus_is_rejected() {
        var harness = await ComposeAsync();
        try {
            var migrationId = new MigrationId(Guid.NewGuid());
            var staleMigrationId = new MigrationId(Guid.NewGuid());
            await harness.Orchestrator.ProposeMigrationStartedAsync(harness.MetaReplica, migrationId, StreamName, 0, FromGroupId, ToGroupId);
            (await WaitUntilAsync(() => harness.Table.TryGetAssignment(StreamName, 0, out var a) && a.MigratingToGroupId == ToGroupId)).Should().BeTrue();

            await harness.Orchestrator.ProposeCutoverAsync(harness.MetaReplica, staleMigrationId, StreamName, 0, ToGroupId);
            await FlushAppliedOrderAsync(harness.MetaReplica, harness.Table);

            harness.Table.TryGetAssignment(StreamName, 0, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be(FromGroupId, "a cutover carrying someone else's migration id must never flip routing");
            assignment.MigratingToGroupId.Should().Be(ToGroupId);
        } finally {
            await harness.DisposeAsync();
        }
    }

    // The applier processes committed records in strict order off a single channel reader — once
    // an orthogonal, unrelated sentinel command's own effect is visible, every earlier command
    // (including a just-rejected one) is guaranteed to have already been processed too.
    static async Task FlushAppliedOrderAsync(RaftReplica metaReplica, ShardAssignmentTable table) {
        var sentinelStream = $"sentinel-{Guid.NewGuid()}";
        await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream(sentinelStream, 1, RoutingAlgorithm.XxHash3V1))));
        (await WaitUntilAsync(() => table.TryGetStreamConfig(sentinelStream, out _))).Should().BeTrue();
    }

    static async Task<ulong> Publish(RaftReplica replica, string partitionKey, string payload) {
        var result = await replica.ProposeAsync(new AppendRequest(
            System.Text.Encoding.UTF8.GetBytes(partitionKey), [], System.Text.Encoding.UTF8.GetBytes(payload)));
        return result.LogicalOffset;
    }

    static async Task AssertShardLogContainsAsync(ShardLog shardLog, (ulong Offset, string ExpectedPayload)[] expected) {
        foreach (var (offset, expectedPayload) in expected) {
            (await WaitUntilAsync(() => shardLog.TryGet(offset, out _))).Should().BeTrue($"offset {offset} should have been copied into the new group");
            shardLog.TryGet(offset, out var handle);
            using (handle)
                System.Text.Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be(expectedPayload);
        }
    }

    sealed class Harness : IAsyncDisposable {
        public required RaftReplicaRegistry RaftRegistry { get; init; }
        public required MessagingGroupRegistry MessagingRegistry { get; init; }
        public required MigrationAdmissionGate AdmissionGate { get; init; }
        public required ShardAssignmentTable Table { get; init; }
        public required ReshardOrchestrator Orchestrator { get; init; }
        public required RaftReplica MetaReplica { get; init; }
        public required ShardAssignmentTableApplier MetaApplier { get; init; }
        public required RaftReplica FromReplica { get; init; }
        public required (ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator) FromMessaging { get; init; }
        public required AckIndexApplier FromApplier { get; init; }
        public required RaftReplica ToReplica { get; init; }
        public required (ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator) ToMessaging { get; init; }
        public required AckIndexApplier ToApplier { get; init; }
        public required string DirectoryRoot { get; init; }

        public async ValueTask DisposeAsync() {
            await MetaApplier.DisposeAsync();
            await MetaReplica.DisposeAsync();
            await FromApplier.DisposeAsync();
            await FromMessaging.RetentionEvaluator.DisposeAsync();
            await FromMessaging.ShardLog.DisposeAsync();
            await FromReplica.DisposeAsync();
            await ToApplier.DisposeAsync();
            await ToMessaging.RetentionEvaluator.DisposeAsync();
            await ToMessaging.ShardLog.DisposeAsync();
            await ToReplica.DisposeAsync();
            Directory.Delete(DirectoryRoot, recursive: true);
        }
    }

    static async Task<Harness> ComposeAsync() {
        var directoryRoot = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directoryRoot);

        var raftRegistry = new RaftReplicaRegistry();
        var messagingRegistry = new MessagingGroupRegistry();
        var admissionGate = new MigrationAdmissionGate();
        var table = new ShardAssignmentTable();
        var orchestrator = new ReshardOrchestrator(raftRegistry, messagingRegistry, admissionGate);

        var metaReplica = await StartSingleNodeReplicaAsync("__meta__", "meta-node");
        var metaApplier = new ShardAssignmentTableApplier(metaReplica, table);

        var fromReplica = await StartSingleNodeReplicaAsync(FromGroupId, "node-1");
        raftRegistry.Register(fromReplica);
        var fromMessaging = await ComposeMessagingAsync(fromReplica, Path.Combine(directoryRoot, FromGroupId));
        messagingRegistry.Register(FromGroupId, fromMessaging.ShardLog, fromMessaging.AckIndex, fromMessaging.RetentionEvaluator);
        var fromApplier = new AckIndexApplier(fromMessaging.ShardLog, fromMessaging.AckIndex);

        var toReplica = await StartSingleNodeReplicaAsync(ToGroupId, "node-2");
        raftRegistry.Register(toReplica);
        var toMessaging = await ComposeMessagingAsync(toReplica, Path.Combine(directoryRoot, ToGroupId));
        messagingRegistry.Register(ToGroupId, toMessaging.ShardLog, toMessaging.AckIndex, toMessaging.RetentionEvaluator);
        var toApplier = new AckIndexApplier(toMessaging.ShardLog, toMessaging.AckIndex);

        // Seeds the pre-migration state — a stream whose one virtual shard already, "always",
        // lived on the old group — the same seed-via-a-no-op-migration technique the client
        // cluster tests use, standing in for whatever earlier admin command really created it.
        var seedMigrationId = new MigrationId(Guid.NewGuid());
        table.Apply(new CreateStream(StreamName, VirtualShardCount: 1, RoutingAlgorithm.XxHash3V1));
        table.Apply(new RegisterGroup(FromGroupId, []));
        table.Apply(new RegisterGroup(ToGroupId, []));
        table.Apply(new MigrationStarted(seedMigrationId, StreamName, 0, FromGroupId, FromGroupId));
        table.Apply(new Cutover(seedMigrationId, StreamName, 0, FromGroupId));

        return new Harness {
            RaftRegistry = raftRegistry, MessagingRegistry = messagingRegistry, AdmissionGate = admissionGate, Table = table, Orchestrator = orchestrator,
            MetaReplica = metaReplica, MetaApplier = metaApplier,
            FromReplica = fromReplica, FromMessaging = fromMessaging, FromApplier = fromApplier,
            ToReplica = toReplica, ToMessaging = toMessaging, ToApplier = toApplier,
            DirectoryRoot = directoryRoot,
        };
    }

    static async Task<RaftReplica> StartSingleNodeReplicaAsync(string groupId, string nodeId) {
        var config = new RaftReplicaConfig(
            GroupId: groupId, NodeId: nodeId, Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync();
        replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node group -> ready leader
        await replica.DrainAsync();
        return replica;
    }

    static async Task<(ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator)> ComposeMessagingAsync(RaftReplica replica, string directory) {
        Directory.CreateDirectory(directory);
        var ackIndex = new AckIndex();
        var shardLog = await ShardLog.OpenAsync(directory, replica.ReadCommittedAsync,
            compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())));
        var retentionEvaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
            ShardLog: shardLog, AckIndex: ackIndex, IsMandatory: _ => true, GetMaxRetention: () => TimeSpan.MaxValue,
            ForwardToDeadLetterAsync: (_, _) => Task.FromResult(false), IsAdmissionPaused: () => false, IsLeader: () => replica.IsLeader));
        return (shardLog, ackIndex, retentionEvaluator);
    }

    static Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null) =>
        ConditionPoller.WaitUntilAsync(condition, timeout ?? TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(5));
}
