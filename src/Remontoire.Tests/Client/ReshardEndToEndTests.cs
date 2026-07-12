using System.Net;
using System.Text;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Grpc.Net.Client;
using Remontoire.Messaging;
using Remontoire.Meta.V1;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Server;
using Remontoire.Server.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Client;

// Layer 4: real gRPC, real Kestrel hosts, one process — a meta-group node plus two real,
// separate physical data groups — driving the full reshard protocol (ReshardOrchestrator, the
// same class the layer-3 reshard tests already exercise) against a live, actively-publishing
// RemontoireConnection. This is the direct end-to-end verification of the exit criterion: no
// message loss, no downtime, the ShardMigrating contention path exercised over a real connection,
// and ShardRouter.GetVirtualShardIndex genuinely deciding where a message lands.
[Collection("RealNetwork")]
public class ReshardEndToEndTests {
    const string StreamName = "orders";
    const string FromGroupId = "group-1";
    const string ToGroupId = "group-2";

    sealed class DataGroupNode : IAsyncDisposable {
        public required WebApplication Host { get; init; }
        public required RaftReplica Replica { get; init; }
        public required ShardLog ShardLog { get; init; }
        public required AckIndex AckIndex { get; init; }
        public required AckIndexApplier Applier { get; init; }
        public required GrpcChannel WatcherChannel { get; init; }
        public required ShardAssignmentWatcher Watcher { get; init; }

        public async ValueTask DisposeAsync() {
            await Watcher.DisposeAsync();
            WatcherChannel.Dispose();
            await Applier.DisposeAsync();
            await AckIndex.DisposeAsync();
            await ShardLog.DisposeAsync();
            await Replica.DisposeAsync();
            await Host.DisposeAsync();
        }
    }

    static async Task<WebApplication> StartDataHostAsync() {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

        builder.Services.AddGrpc();
        builder.Services.AddSingleton<RaftReplicaRegistry>();
        builder.Services.AddSingleton<MessagingGroupRegistry>();
        builder.Services.AddSingleton<LeaderAddressDirectory>();
        builder.Services.AddSingleton<ShardAssignmentTable>();
        builder.Services.AddSingleton<MigrationAdmissionGate>();

        var app = builder.Build();
        app.MapGrpcService<RaftTransportGrpcService>();
        app.MapGrpcService<RemontoireClientGrpcService>();
        await app.StartAsync();
        return app;
    }

    static async Task<WebApplication> StartMetaHostAsync() {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

        builder.Services.AddGrpc();
        builder.Services.AddSingleton<MetaLogJournal>();

        var app = builder.Build();
        app.MapGrpcService<ShardAssignmentMetaGrpcService>();
        await app.StartAsync();
        return app;
    }

    static async Task<DataGroupNode> StartDataGroupAsync(string groupId, string directory, Uri metaSeedAddress) {
        Directory.CreateDirectory(directory);
        var host = await StartDataHostAsync();

        var config = new RaftReplicaConfig(
            GroupId: groupId, NodeId: $"{groupId}-node", Peers: [],
            HeartbeatInterval: TimeSpan.FromMilliseconds(50), ElectionTimeoutMin: TimeSpan.FromMilliseconds(150), ElectionTimeoutMax: TimeSpan.FromMilliseconds(300));

        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync();
        replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node group -> ready leader
        await replica.DrainAsync();
        host.Services.GetRequiredService<RaftReplicaRegistry>().Register(replica);

        var ackIndex = new AckIndex();
        var shardLog = await ShardLog.OpenAsync(directory, replica.ReadCommittedAsync,
            compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())));
        var applier = new AckIndexApplier(shardLog, ackIndex);
        host.Services.GetRequiredService<MessagingGroupRegistry>().Register(groupId, shardLog, ackIndex);

        // A real watcher, pointed at the real meta host — this node's own routing table is kept
        // fresh exactly the way a production node without meta-group membership would be. A short
        // reconciliation interval (ShardAssignmentWatcher's own documented defense-in-depth against
        // a live Watch stream silently stalling without visibly failing) — the 2-minute production
        // default would never kick in during this test's own, much shorter timeout budget.
        var watcherChannel = GrpcChannel.ForAddress(metaSeedAddress);
        var watcher = new ShardAssignmentWatcher(new ShardAssignmentMeta.ShardAssignmentMetaClient(watcherChannel), host.Services.GetRequiredService<ShardAssignmentTable>(),
            reconciliationInterval: TimeSpan.FromMilliseconds(200));

        return new DataGroupNode { Host = host, Replica = replica, ShardLog = shardLog, AckIndex = ackIndex, Applier = applier, WatcherChannel = watcherChannel, Watcher = watcher };
    }

    static async Task<bool> RunUntilAsync(Func<bool> condition, TimeSpan timeout) {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline) {
            if (condition())
                return true;
            await Task.Delay(20);
        }
        return condition();
    }

    [Fact]
    public async Task An_operator_can_live_reshard_a_stream_without_losing_messages_or_downtime() {
        var directoryRoot = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directoryRoot);

        var metaHost = await StartMetaHostAsync();
        var metaSeedAddress = new Uri(metaHost.Urls.First());

        var metaConfig = new RaftReplicaConfig(
            GroupId: "__meta__", NodeId: "meta-node", Peers: [],
            HeartbeatInterval: TimeSpan.FromMilliseconds(50), ElectionTimeoutMin: TimeSpan.FromMilliseconds(150), ElectionTimeoutMax: TimeSpan.FromMilliseconds(300));
        var metaReplica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), metaConfig);
        await metaReplica.StartAsync();
        metaReplica.TryPost(new ElectionTimeoutElapsed(metaReplica.ElectionTimerGeneration));
        await metaReplica.DrainAsync();

        var metaJournal = metaHost.Services.GetRequiredService<MetaLogJournal>();
        var metaSideTable = new ShardAssignmentTable(); // the applier needs a table to feed; this host serves none of its own routing off it
        var metaApplier = new ShardAssignmentTableApplier(metaReplica, metaSideTable, metaJournal);

        var fromGroup = await StartDataGroupAsync(FromGroupId, Path.Combine(directoryRoot, FromGroupId), metaSeedAddress);
        var toGroup = await StartDataGroupAsync(ToGroupId, Path.Combine(directoryRoot, ToGroupId), metaSeedAddress);

        try {
            var fromAddress = new Uri(fromGroup.Host.Urls.First());
            var toAddress = new Uri(toGroup.Host.Urls.First());

            // A redirect built from the assignment table's own PhysicalGroupDescriptor still
            // resolves a node id to an address via each host's own LeaderAddressDirectory —
            // normally populated from that host's own configured peers. Neither group is a peer
            // of the other here (each is single-node), so a cross-group redirect needs both
            // directions registered explicitly, the same information RaftReplicaHostedService
            // would derive from configuration in production.
            fromGroup.Host.Services.GetRequiredService<LeaderAddressDirectory>().Register("to", toAddress);
            toGroup.Host.Services.GetRequiredService<LeaderAddressDirectory>().Register("from", fromAddress);

            var seedMigrationId = new MigrationId(Guid.NewGuid());
            await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream(StreamName, 1, RoutingAlgorithm.XxHash3V1))));
            await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new RegisterGroup(FromGroupId, [new ShardGroupMember("from", fromAddress)]))));
            await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new RegisterGroup(ToGroupId, [new ShardGroupMember("to", toAddress)]))));
            await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new MigrationStarted(seedMigrationId, StreamName, 0, FromGroupId, FromGroupId))));
            await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new Cutover(seedMigrationId, StreamName, 0, FromGroupId))));

            // Explicit precondition, not folded into PublishOnceReadyAsync's own retry loop:
            // proves fromGroup's OWN watcher (a separate instance from the connection's) actually
            // caught up on its server-side table before any publish is attempted. If this ever
            // times out, it isolates the failure to the watcher/journal path specifically, rather
            // than leaving it indistinguishable from any other publish-retry exhaustion.
            // Checks BOTH TryGetStreamConfig (fed by CreateStream) and TryGetAssignment (fed by
            // MigrationStarted/Cutover) — two independent dictionaries inside ShardAssignmentTable
            // with no cross-validation, so one becoming visible never guarantees the other already
            // is (confirmed via a real CI failure: Publish's own NotFound check, which reads
            // TryGetStreamConfig first, fired even though an assignment-only precondition here had
            // already passed).
            var fromGroupTable = fromGroup.Host.Services.GetRequiredService<ShardAssignmentTable>();
            (await RunUntilAsync(() => fromGroupTable.TryGetStreamConfig(StreamName, out _) && fromGroupTable.TryGetAssignment(StreamName, 0, out var a) && a.GroupId == FromGroupId, TimeSpan.FromSeconds(30)))
                .Should().BeTrue($"fromGroup's own watcher must see the stream config and the group assignment before any publish can ever succeed — " +
                    $"last applied version: {fromGroup.Watcher.LastAppliedVersion}, last watcher failure: {fromGroup.Watcher.LastFailure}");

            using var connection = new RemontoireConnection(new RemontoireClientOptions(
                MetaGroupSeedAddresses: [metaSeedAddress], MaxRedirectAttempts: 20, RedirectRetryDelay: TimeSpan.FromMilliseconds(50)));

            var published = new List<(long Offset, string Payload)>();
            for (var i = 0; i < 5; i++) {
                var payload = $"pre-{i}";
                var result = await PublishOnceReadyAsync(connection, payload);
                published.Add((result.Offset, payload));
            }

            // Drives the reshard, using the SAME orchestrator the layer-3 reshard tests already
            // exercise — the "bare operator tool" this phase needs, standing in for a later
            // phase's real, authorized admin-API surface.
            var orchestrator = new ReshardOrchestrator(
                fromGroup.Host.Services.GetRequiredService<RaftReplicaRegistry>(), fromGroup.Host.Services.GetRequiredService<MessagingGroupRegistry>(),
                fromGroup.Host.Services.GetRequiredService<MigrationAdmissionGate>());
            // CopyRecordsAsync needs both groups resolvable from ONE registry pair — register
            // toGroup's replica into fromGroup's own RaftReplicaRegistry too, purely so this one
            // orchestrator instance can reach both sides (a real multi-group node would already
            // have every locally-hosted group in the same registry, as RaftReplicaHostedService
            // already builds for N groups on one process).
            fromGroup.Host.Services.GetRequiredService<RaftReplicaRegistry>().Register(toGroup.Replica);

            var migrationId = new MigrationId(Guid.NewGuid());
            await orchestrator.ProposeMigrationStartedAsync(metaReplica, migrationId, StreamName, 0, FromGroupId, ToGroupId);
            (await RunUntilAsync(() => fromGroup.Host.Services.GetRequiredService<ShardAssignmentTable>().TryGetAssignment(StreamName, 0, out var a) && a.MigratingToGroupId == ToGroupId, TimeSpan.FromSeconds(5)))
                .Should().BeTrue();

            var copiedUpTo = await orchestrator.CopyRecordsAsync(FromGroupId, ToGroupId, 0);

            // More traffic lands on the old group WHILE the copy is already underway — proving
            // it keeps accepting writes throughout, not just before the migration started.
            for (var i = 5; i < 8; i++) {
                var payload = $"mid-{i}";
                var result = await PublishOnceReadyAsync(connection, payload);
                published.Add((result.Offset, payload));
            }

            (await RunUntilAsync(() => fromGroup.ShardLog.TryGet((ulong)published[^1].Offset, out _), TimeSpan.FromSeconds(5))).Should().BeTrue();
            copiedUpTo = await orchestrator.CopyRecordsAsync(FromGroupId, ToGroupId, copiedUpTo); // tail catch-up

            // The pause must stay active until BOTH cutover has committed AND this group's own
            // local table has learned about it — resuming any earlier reopens the exact window
            // that would let a write land here with no further copy round left to save it, since
            // the meta-group already considers this shard moved.
            Task<PublishResult>? duringPause;
            var fromTable = fromGroup.Host.Services.GetRequiredService<ShardAssignmentTable>();
            using (orchestrator.PauseAdmission(FromGroupId)) {
                // A publish attempted WHILE paused must not fail — it retries via the
                // ShardMigrating contention path and only completes once the pause lifts below.
                duringPause = connection.PublishAsync(StreamName, "key-paused", "during-pause"u8.ToArray());
                (await Task.WhenAny(duringPause, Task.Delay(TimeSpan.FromMilliseconds(200)))).Should().NotBe(duringPause, "a paused group must keep retrying, not fail or return immediately");

                await orchestrator.ProposeCutoverAsync(metaReplica, migrationId, StreamName, 0, ToGroupId);
                (await RunUntilAsync(() => fromTable.TryGetAssignment(StreamName, 0, out var a) && a.GroupId == ToGroupId, TimeSpan.FromSeconds(5)))
                    .Should().BeTrue();
            }

            await orchestrator.ProposeMigrationCompletedAsync(metaReplica, migrationId, StreamName, 0);

            var pausedResult = await duringPause.WaitAsync(TimeSpan.FromSeconds(10));
            published.Add((pausedResult.Offset, "during-pause"));

            // The client's own watcher needs a moment to learn the cutover before these resolve
            // against the new group — PublishOnceReadyAsync's own retry loop absorbs that gap,
            // same as it already does for the "watcher hasn't caught up yet at all" case.
            for (var i = 0; i < 3; i++) {
                var payload = $"post-{i}";
                var result = await PublishOnceReadyAsync(connection, payload);
                published.Add((result.Offset, payload));
            }

            // Every published message must now be readable from the NEW group — proving nothing
            // was lost across the whole sequence, and that publishing after cutover really landed
            // on the new group, not silently still on the old one.
            foreach (var (offset, payload) in published) {
                (await RunUntilAsync(() => toGroup.ShardLog.TryGet((ulong)offset, out _), TimeSpan.FromSeconds(5)))
                    .Should().BeTrue($"offset {offset} ('{payload}') should have ended up on the new group");
                toGroup.ShardLog.TryGet((ulong)offset, out var handle);
                using (handle)
                    Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be(payload);
            }

            published.Select(p => p.Offset).Should().OnlyHaveUniqueItems("no offset should ever be reused across the reshard");
        } finally {
            await metaApplier.DisposeAsync();
            await metaReplica.DisposeAsync();
            await metaHost.DisposeAsync();
            await fromGroup.DisposeAsync();
            await toGroup.DisposeAsync();
            Directory.Delete(directoryRoot, recursive: true);
        }
    }

    static async Task<PublishResult> PublishOnceReadyAsync(RemontoireConnection connection, string payload) {
        // A generous budget: real Kestrel-host startup and the meta-group watcher's own catch-up
        // can genuinely take longer than 5s under CI's more constrained CPU (confirmed — this
        // exact call failed consistently in CI at the 5s mark, never locally).
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (true) {
            try {
                return await connection.PublishAsync(StreamName, "key", Encoding.UTF8.GetBytes(payload));
            } catch (Exception ex) when (ex is ArgumentException or RemontoireUnavailableException && DateTime.UtcNow < deadline) {
                await Task.Delay(20);
            }
        }
    }
}
