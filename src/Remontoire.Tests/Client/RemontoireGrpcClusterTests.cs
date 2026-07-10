using System.Net;
using System.Text;
using FluentAssertions;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire.Client.V1;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Server;
using Remontoire.Server.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Client;

// Real gRPC, over real loopback sockets, between real Kestrel hosts — one process, three
// in-process "nodes", each a full RaftReplica+ShardLog+AckIndex composition (exactly what
// RaftReplicaHostedService builds), plus one single-node meta-group replica on the first host, with
// a real RemontoireConnection on top. Nothing is mocked — only the process boundary is collapsed,
// the same tradeoff RaftGrpcClusterTests already makes.
[Collection("RealNetwork")]
public class RemontoireGrpcClusterTests {
    const string GroupId = "group-1";

    // Must equal GroupId: RemontoireClientGrpcService.Resolve treats this stream's single virtual
    // shard as always assigned to GroupId — set up below by seeding the meta-group with a matching
    // CreateStream/RegisterGroup/Cutover, the same commands a real control plane would commit.
    const string StreamName = GroupId;

    static async Task<bool> RunUntilAsync(Func<bool> condition, TimeSpan timeout) {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline) {
            if (condition())
                return true;
            await Task.Delay(20);
        }
        return condition();
    }

    static readonly TimeSpan FastTickInterval = TimeSpan.FromMilliseconds(200);

    sealed class Node : IAsyncDisposable {
        public required WebApplication Host { get; init; }
        public required RaftReplica Replica { get; init; }
        public required RaftGrpcTransport Transport { get; init; }
        public required string DataDirectory { get; init; }
        public required ShardLog ShardLog { get; init; }
        public required AckIndex AckIndex { get; init; }
        public required AckIndexApplier Applier { get; init; }
        public required AckCheckpointer AckCheckpointer { get; init; }
        public required RetentionEvaluator RetentionEvaluator { get; init; }

        // Only set on the one node that also hosts the meta-group (index 0) — every
        // RemontoireConnection in this test file bootstraps from that node's address.
        public RaftReplica? MetaReplica { get; init; }
        public ShardAssignmentTableApplier? MetaApplier { get; init; }

        public async ValueTask DisposeAsync() {
            if (MetaApplier is not null)
                await MetaApplier.DisposeAsync();
            if (MetaReplica is not null)
                await MetaReplica.DisposeAsync();

            await AckCheckpointer.DisposeAsync();
            await RetentionEvaluator.DisposeAsync();
            await Applier.DisposeAsync();
            await ShardLog.DisposeAsync();
            await Replica.DisposeAsync();
            Transport.Dispose();
            await Host.DisposeAsync();
        }
    }

    static async Task<WebApplication> StartHostAsync() {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

        builder.Services.AddGrpc();
        builder.Services.AddSingleton<RaftReplicaRegistry>();
        builder.Services.AddSingleton<MessagingGroupRegistry>();
        builder.Services.AddSingleton<LeaderAddressDirectory>();
        builder.Services.AddSingleton<ShardAssignmentTable>();
        builder.Services.AddSingleton<MetaLogJournal>();
        builder.Services.AddSingleton<MigrationAdmissionGate>();

        var app = builder.Build();
        app.MapGrpcService<RaftTransportGrpcService>();
        app.MapGrpcService<RemontoireClientGrpcService>();
        app.MapGrpcService<ShardAssignmentMetaGrpcService>();
        await app.StartAsync();
        return app;
    }

    static async Task<List<Node>> StartClusterAsync(string directoryRoot, long flushThresholdBytes = 64 * 1024 * 1024) {
        var hosts = await Task.WhenAll(StartHostAsync(), StartHostAsync(), StartHostAsync());
        var nodeIds = new[] { "node-1", "node-2", "node-3" };
        var members = nodeIds.Zip(hosts, (nodeId, host) => new RaftGroupMember(nodeId, new Uri(host.Urls.First()))).ToArray();

        var nodes = new List<Node>();
        for (var i = 0; i < hosts.Length; i++) {
            var nodeId = nodeIds[i];
            var peers = members.Where(member => member.NodeId != nodeId).ToArray();
            var config = new RaftReplicaConfig(
                GroupId: GroupId, NodeId: nodeId, Peers: peers,
                HeartbeatInterval: TimeSpan.FromMilliseconds(50),
                ElectionTimeoutMin: TimeSpan.FromMilliseconds(150),
                ElectionTimeoutMax: TimeSpan.FromMilliseconds(300));

            var transport = new RaftGrpcTransport(peers, config.ResolvedRpcTimeout);
            var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), transport, config);
            await replica.StartAsync();
            hosts[i].Services.GetRequiredService<RaftReplicaRegistry>().Register(replica);

            var leaderAddresses = hosts[i].Services.GetRequiredService<LeaderAddressDirectory>();
            foreach (var peer in peers)
                leaderAddresses.Register(peer.NodeId, peer.Address);

            var ackIndex = new AckIndex();
            var admissionGate = hosts[i].Services.GetRequiredService<MigrationAdmissionGate>();
            var table = hosts[i].Services.GetRequiredService<ShardAssignmentTable>();

            // Assigned right after construction, below — RetentionEvaluator's own constructor
            // needs an already-open ShardLog, so this can't be resolved directly the way
            // RaftReplicaHostedService resolves it lazily through a dictionary keyed by groupId.
            // Same reasoning, a captured local instead of a dictionary lookup since this harness
            // only ever has the one group.
            RetentionEvaluator? retentionEvaluatorRef = null;

            var directory = Path.Combine(directoryRoot, nodeId);
            Directory.CreateDirectory(directory);
            var shardLog = await ShardLog.OpenAsync(directory, replica.ReadCommittedAsync, flushThresholdBytes: flushThresholdBytes,
                compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null,
                    GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(retentionEvaluatorRef!.SafeToPruneWatermark),
                    RetentionTickInterval: FastTickInterval),
                retentionPolicy: new RetentionPolicy(
                    GetMaxTotalBytesPerVirtualShard: () => table.GetRetentionPolicy(StreamName).MaxSizeBytesPerVirtualShard,
                    IsAdmissionPaused: () => admissionGate.IsPaused(GroupId),
                    SizePruneTickInterval: FastTickInterval));
            var applier = new AckIndexApplier(shardLog, ackIndex);
            hosts[i].Services.GetRequiredService<MessagingGroupRegistry>().Register(GroupId, shardLog, ackIndex);

            var ackCheckpointer = new AckCheckpointer(new AckCheckpointerOptions(
                ackIndex,
                ProposeCheckpointAsync: (consumerGroup, watermark, ct) => replica.ProposeAsync(new AckCheckpointRequest(consumerGroup, watermark), ct).AsTask(),
                IsLeader: () => replica.IsLeader,
                IsCheckpointMode: consumerGroup => table.GetConsumerGroupPolicy(StreamName, consumerGroup).Mode == AckMode.Checkpoint,
                GetCheckpointThresholds: () => (table.GetRetentionPolicy(StreamName).CheckpointInterval, table.GetRetentionPolicy(StreamName).CheckpointOffsetCount),
                IsAdmissionPaused: () => admissionGate.IsPaused(GroupId)));

            var retentionEvaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex,
                IsMandatory: consumerGroup => table.GetConsumerGroupPolicy(StreamName, consumerGroup).Mandatory,
                GetMaxRetention: () => table.GetRetentionPolicy(StreamName).MaxRetention,
                ForwardToDeadLetterAsync: (request, ct) => ForwardToDeadLetterAsync(table, replica, request, ct),
                IsAdmissionPaused: () => admissionGate.IsPaused(GroupId),
                TickInterval: FastTickInterval));
            retentionEvaluatorRef = retentionEvaluator;

            // Every node's own table needs to already agree on the one stream/group/assignment
            // this whole test file uses — hosts other than 0 have no meta-group replica of their
            // own here, so seed them directly, the same end state a watcher would arrive at.
            var seedMigrationId = new MigrationId(Guid.NewGuid());
            table.Apply(new CreateStream(StreamName, VirtualShardCount: 1, RoutingAlgorithm.XxHash3V1));
            table.Apply(new RegisterGroup(GroupId, members.Select(member => new ShardGroupMember(member.NodeId, member.Address)).ToArray()));
            table.Apply(new MigrationStarted(seedMigrationId, StreamName, VirtualShardIndex: 0, FromGroupId: GroupId, ToGroupId: GroupId));
            table.Apply(new Cutover(seedMigrationId, StreamName, VirtualShardIndex: 0, GroupId));

            RaftReplica? metaReplica = null;
            ShardAssignmentTableApplier? metaApplier = null;
            if (i == 0) {
                // A real, single-node meta-group on this one host, so a real RemontoireConnection
                // has an actual Watch/GetSnapshot endpoint to bootstrap its own table from — not
                // just the direct table.Apply seeding above, which only ever helps this node's own
                // server-side Resolve, never a client's.
                var metaConfig = new RaftReplicaConfig(
                    GroupId: "__meta__", NodeId: "meta-node", Peers: [],
                    HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

                metaReplica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), metaConfig);
                await metaReplica.StartAsync();
                metaReplica.TryPost(new ElectionTimeoutElapsed(metaReplica.ElectionTimerGeneration)); // single-node -> ready leader
                await metaReplica.DrainAsync();

                var journal = hosts[i].Services.GetRequiredService<MetaLogJournal>();
                metaApplier = new ShardAssignmentTableApplier(metaReplica, table, journal);

                var metaSeedMigrationId = new MigrationId(Guid.NewGuid());
                await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream(StreamName, 1, RoutingAlgorithm.XxHash3V1))));
                await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [],
                    MetaLogRecord.Encode(new RegisterGroup(GroupId, members.Select(member => new ShardGroupMember(member.NodeId, member.Address)).ToArray()))));
                await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [],
                    MetaLogRecord.Encode(new MigrationStarted(metaSeedMigrationId, StreamName, 0, GroupId, GroupId))));
                await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new Cutover(metaSeedMigrationId, StreamName, 0, GroupId))));

                await RunUntilAsync(() => journal.Snapshot().Records.Count == 4, TimeSpan.FromSeconds(2));
            }

            nodes.Add(new Node {
                Host = hosts[i], Replica = replica, Transport = transport, DataDirectory = directory, ShardLog = shardLog, AckIndex = ackIndex, Applier = applier,
                AckCheckpointer = ackCheckpointer, RetentionEvaluator = retentionEvaluator,
                MetaReplica = metaReplica, MetaApplier = metaApplier,
            });
        }

        return nodes;
    }

    // Mirrors RaftReplicaHostedService.ForwardToDeadLetterAsync, simplified for this harness'
    // single-group topology (no cross-group routing needed — the dead-letter stream's own virtual
    // shard is always assigned to the same GroupId every other stream in this file uses).
    static async Task ForwardToDeadLetterAsync(ShardAssignmentTable table, RaftReplica replica, AppendRequest request, CancellationToken cancellationToken) {
        var deadLetterStreamName = $"{StreamName}.__deadletter__";
        if (!table.TryGetStreamConfig(deadLetterStreamName, out var config))
            return;

        var virtualShardIndex = ShardRouter.GetVirtualShardIndex(request.PartitionKey.Span, config.VirtualShardCount, config.RoutingAlgorithm);
        if (!table.TryGetAssignment(deadLetterStreamName, virtualShardIndex, out var assignment) || assignment.GroupId != GroupId)
            return;

        await replica.ProposeAsync(request, cancellationToken);
    }

    static RemontoireConnection Connect(IEnumerable<Node> nodes) =>
        new(new RemontoireClientOptions(
            MetaGroupSeedAddresses: [new Uri(nodes.First().Host.Urls.First())],
            MaxRedirectAttempts: 10,
            RedirectRetryDelay: TimeSpan.FromMilliseconds(50)));

    // Overwrites GroupId's membership, as seen from node's own table, down to node's own address
    // only — reproducing "this connection has never heard of any other member" now that a client
    // always learns a group's full membership from the table rather than being handed an address
    // list directly. Reuses node's own already-running meta-group replica if it has one (index 0
    // in every StartClusterAsync-built cluster); otherwise stands up a small dedicated one, since
    // every host maps ShardAssignmentMetaGrpcService but only index 0 starts with a fed journal.
    static async Task<(RaftReplica? Replica, ShardAssignmentTableApplier? Applier)> SeedSoleKnownMemberAsync(Node node) {
        var registerGroup = new RegisterGroup(GroupId, [new ShardGroupMember("solo", new Uri(node.Host.Urls.First()))]);
        var table = node.Host.Services.GetRequiredService<ShardAssignmentTable>();

        if (node.MetaReplica is { } existingMetaReplica) {
            await existingMetaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(registerGroup)));
            await RunUntilAsync(() => table.TryGetGroup(GroupId, out var group) && group.Members.Count == 1, TimeSpan.FromSeconds(2));
            return (null, null);
        }

        var metaConfig = new RaftReplicaConfig(
            GroupId: "__meta__", NodeId: "meta-node-solo", Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), metaConfig);
        await replica.StartAsync();
        replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node -> ready leader
        await replica.DrainAsync();

        var journal = node.Host.Services.GetRequiredService<MetaLogJournal>();
        var applier = new ShardAssignmentTableApplier(replica, table, journal);

        var soloMigrationId = new MigrationId(Guid.NewGuid());
        await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream(StreamName, 1, RoutingAlgorithm.XxHash3V1))));
        await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(registerGroup)));
        await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new MigrationStarted(soloMigrationId, StreamName, 0, GroupId, GroupId))));
        await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new Cutover(soloMigrationId, StreamName, 0, GroupId))));

        await RunUntilAsync(() => table.TryGetGroup(GroupId, out var group) && group.Members.Count == 1, TimeSpan.FromSeconds(2));
        return (replica, applier);
    }

    // A freshly constructed RemontoireConnection's own ShardAssignmentWatcher needs a moment to
    // catch up via its initial GetSnapshot before the stream/assignment it needs is known —
    // exactly the race RemontoireConnection.ResolveGroup's own ArgumentException/RemontoireUnavailableException
    // choices anticipate. Retries the first publish in each test until that catch-up has happened.
    static async Task<PublishResult> PublishOnceReadyAsync(RemontoireConnection connection, string streamName, string partitionKey, byte[] payload) {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (true) {
            try {
                return await connection.PublishAsync(streamName, partitionKey, payload);
            } catch (Exception ex) when (ex is ArgumentException or RemontoireUnavailableException && DateTime.UtcNow < deadline) {
                await Task.Delay(20);
            }
        }
    }

    // Same catch-up race as PublishOnceReadyAsync, for a freshly constructed connection's very
    // first ConsumeAsync call.
    static async Task<List<RemontoireMessage>> ConsumeOnceReadyAsync(RemontoireConnection connection, string streamName, string consumerGroup, int count, CancellationToken cancellationToken) {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (true) {
            try {
                var messages = new List<RemontoireMessage>();
                await foreach (var message in connection.ConsumeAsync(streamName, consumerGroup, cancellationToken)) {
                    messages.Add(message);
                    if (messages.Count == count)
                        return messages;
                }
                return messages;
            } catch (Exception ex) when (ex is ArgumentException or RemontoireUnavailableException && DateTime.UtcNow < deadline) {
                await Task.Delay(20, cancellationToken);
            }
        }
    }

    static async Task DisposeAllAsync(IEnumerable<Node> nodes, string directoryRoot) {
        foreach (var node in nodes)
            await node.DisposeAsync();
        Directory.Delete(directoryRoot, recursive: true);
    }

    static string CreateTempDirectory() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }

    [Fact]
    public async Task Publish_consume_and_ack_work_end_to_end_over_real_grpc() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();

            using var connection = Connect(nodes);
            var result = await PublishOnceReadyAsync(connection, StreamName, "key-1", "hello"u8.ToArray());
            result.Offset.Should().Be(0);

            var messages = new List<RemontoireMessage>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var message in connection.ConsumeAsync(StreamName, "group-a", cts.Token)) {
                messages.Add(message);
                break;
            }

            messages.Should().HaveCount(1);
            messages[0].Payload.ToArray().Should().Equal("hello"u8.ToArray());

            await connection.AckAsync(StreamName, "group-a", messages[0].ShardId, messages[0].Offset);
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task PublishAsync_receives_ShardMigrating_while_paused_and_succeeds_on_retry_once_it_lifts() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();
            var leader = nodes.First(node => node.Replica.IsLeader);
            var admissionGate = leader.Host.Services.GetRequiredService<MigrationAdmissionGate>();

            using var connection = Connect(nodes);
            await PublishOnceReadyAsync(connection, StreamName, "key-1", "seed"u8.ToArray()); // ensures the watcher has already caught up

            admissionGate.Pause(GroupId);
            var publishTask = connection.PublishAsync(StreamName, "key-2", "hello"u8.ToArray());
            (await Task.WhenAny(publishTask, Task.Delay(TimeSpan.FromMilliseconds(200)))).Should().NotBe(publishTask, "a paused group must keep retrying, not return immediately");

            admissionGate.Resume(GroupId);
            var result = await publishTask.WaitAsync(TimeSpan.FromSeconds(10));

            result.Offset.Should().Be(1, "the retry must eventually reach the same leader and succeed, no message lost");
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task PublishAsync_against_a_follower_redirects_transparently_to_the_real_leader() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();
            var follower = nodes.First(node => !node.Replica.IsLeader);

            var (soloReplica, soloApplier) = await SeedSoleKnownMemberAsync(follower);
            try {
                using var connection = Connect([follower]); // its own table only ever lists the follower's address

                var result = await PublishOnceReadyAsync(connection, StreamName, "key-1", "hello"u8.ToArray());

                result.Offset.Should().Be(0, "the redirect must reach the real leader even though the connection only knew the follower's address");
            } finally {
                if (soloApplier is not null)
                    await soloApplier.DisposeAsync();
                if (soloReplica is not null)
                    await soloReplica.DisposeAsync();
            }
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task ConsumeAsync_against_a_follower_redirects_transparently_to_the_real_leader() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();

            using (var seed = Connect(nodes))
                await PublishOnceReadyAsync(seed, StreamName, "key-1", "hello"u8.ToArray());

            var follower = nodes.First(node => !node.Replica.IsLeader);
            var (soloReplica, soloApplier) = await SeedSoleKnownMemberAsync(follower);
            try {
                using var connection = Connect([follower]); // its own table only ever lists the follower's address

                List<RemontoireMessage> messages;
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                    messages = await ConsumeOnceReadyAsync(connection, StreamName, "group-a", count: 1, cts.Token);

                var expectedPayload = "hello"u8.ToArray();
                messages.Should().ContainSingle(message => message.Payload.ToArray().SequenceEqual(expectedPayload),
                    "the stream must redirect to the real leader even though the connection only knew the follower's address");
            } finally {
                if (soloApplier is not null)
                    await soloApplier.DisposeAsync();
                if (soloReplica is not null)
                    await soloReplica.DisposeAsync();
            }
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task A_restarted_consumer_receives_exactly_the_not_yet_acked_messages_again() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();

            using var connection = Connect(nodes);
            await PublishOnceReadyAsync(connection, StreamName, "key-1", "one"u8.ToArray());
            await connection.PublishAsync(StreamName, "key-2", "two"u8.ToArray());

            var firstBatch = new List<RemontoireMessage>();
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                await foreach (var message in connection.ConsumeAsync(StreamName, "group-a", cts.Token)) {
                    firstBatch.Add(message);
                    if (firstBatch.Count == 2)
                        break;
                }

            firstBatch.Should().HaveCount(2);

            // Acks only the first message — simulates a consumer crashing before acking the second.
            await connection.AckAsync(StreamName, "group-a", firstBatch[0].ShardId, firstBatch[0].Offset);

            var secondBatch = new List<RemontoireMessage>();
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                await foreach (var message in connection.ConsumeAsync(StreamName, "group-a", cts.Token)) {
                    secondBatch.Add(message);
                    break;
                }

            secondBatch.Should().HaveCount(1);
            secondBatch[0].Offset.Should().Be(firstBatch[1].Offset, "the un-acked second message must be redelivered, and only that one");
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task A_leader_crash_during_active_traffic_loses_no_published_or_acked_data() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();

            using var connection = Connect(nodes);
            var first = await PublishOnceReadyAsync(connection, StreamName, "key-1", "one"u8.ToArray());
            await connection.AckAsync(StreamName, "group-a", first.ShardId, first.Offset);

            var leaderNode = nodes.First(node => node.Replica.IsLeader);
            var survivors = nodes.Where(node => node != leaderNode).ToArray();

            // Quorum-committed only guarantees durability, not that every follower's own
            // (asynchronously derived) AckIndex has caught up yet — wait for the surviving nodes
            // specifically, since the leader is about to disappear and won't get the chance.
            (await RunUntilAsync(() => survivors.All(node => node.AckIndex.GetOrCreate("group-a").IsAcked((ulong)first.Offset)), TimeSpan.FromSeconds(10)))
                .Should().BeTrue("the ack must have reached every surviving node before the leader crash, not just a quorum majority that might not include them");

            nodes.Remove(leaderNode);
            await leaderNode.DisposeAsync();

            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10)))
                .Should().BeTrue("the two surviving nodes must elect a new leader");

            var second = await connection.PublishAsync(StreamName, "key-2", "two"u8.ToArray());
            second.Offset.Should().Be(1, "the first message's offset must survive the leader crash, unbroken");

            var messages = new List<RemontoireMessage>();
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                await foreach (var message in connection.ConsumeAsync(StreamName, "group-a", cts.Token)) {
                    messages.Add(message);
                    break; // only the un-acked second message should remain
                }

            messages.Should().ContainSingle(message => message.Offset == second.Offset);
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task Ack_for_a_checkpoint_mode_consumer_group_applies_locally_bypassing_Raft() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();
            SetCheckpointMode(nodes, "checkpoint-group");

            using var connection = Connect(nodes);
            var published = await PublishOnceReadyAsync(connection, StreamName, "key-1", "hello"u8.ToArray());

            await connection.AckAsync(StreamName, "checkpoint-group", published.ShardId, published.Offset);

            var leader = nodes.First(node => node.Replica.IsLeader);
            (await RunUntilAsync(() => leader.AckIndex.GetOrCreate("checkpoint-group").IsAcked((ulong)published.Offset), TimeSpan.FromSeconds(5)))
                .Should().BeTrue();
            leader.AckIndex.GetOrCreate("checkpoint-group").CommittedWatermark.Should().Be(0,
                "checkpoint mode never commits through Raft on its own — only a later AckCheckpointer tick would");
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task Ack_for_a_checkpoint_mode_group_against_a_non_leader_returns_NotLeader() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();
            SetCheckpointMode(nodes, "checkpoint-group");
            var follower = nodes.First(node => !node.Replica.IsLeader);

            using var channel = GrpcChannel.ForAddress(follower.Host.Urls.First());
            var client = new RemontoireClient.RemontoireClientClient(channel);

            var reply = await client.AckAsync(new Remontoire.Client.V1.AckRequest {
                StreamName = StreamName, ConsumerGroup = "checkpoint-group", ShardId = 0, Offsets = { 0UL },
            });

            reply.ResultCase.Should().Be(AckReply.ResultOneofCase.NotLeader,
                "a checkpoint-mode ack still requires the leader — only the replication mechanism is cheaper, not the destination");
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task Consume_for_a_checkpoint_mode_group_succeeds_against_a_follower() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();
            SetCheckpointMode(nodes, "checkpoint-group");

            using var connection = Connect(nodes);
            await PublishOnceReadyAsync(connection, StreamName, "key-1", "hello"u8.ToArray());

            var follower = nodes.First(node => !node.Replica.IsLeader);
            (await RunUntilAsync(() => follower.ShardLog.TryGet(0, out var handle) && Dispose(handle), TimeSpan.FromSeconds(10)))
                .Should().BeTrue("the follower's own ShardLog replays the same committed stream — no reason to wait on the leader for this");

            using var channel = GrpcChannel.ForAddress(follower.Host.Urls.First());
            var client = new RemontoireClient.RemontoireClientClient(channel);
            using var call = client.Consume(new ConsumeRequest { StreamName = StreamName, ConsumerGroup = "checkpoint-group" });
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            (await call.ResponseStream.MoveNext(cts.Token)).Should().BeTrue();
            var reply = call.ResponseStream.Current;

            reply.ResultCase.Should().Be(ConsumeReply.ResultOneofCase.Message,
                "the first time RequiresLeaderPinning ever returns false: a checkpoint-mode consumer may read directly from a follower");
            reply.Message.Payload.ToByteArray().Should().Equal("hello"u8.ToArray());
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    static void SetCheckpointMode(IEnumerable<Node> nodes, string consumerGroup) {
        foreach (var node in nodes)
            node.Host.Services.GetRequiredService<ShardAssignmentTable>().Apply(new SetConsumerGroupAckMode(StreamName, consumerGroup, AckMode.Checkpoint));
    }

    static bool Dispose(LogEntryHandle handle) {
        handle.Dispose();
        return true;
    }

    // Exit criterion: a stuck mandatory group forces a dead-letter forward; a best-effort
    // group's own missing ack never blocks it.
    [Fact]
    public async Task A_stuck_mandatory_group_forces_a_dead_letter_forward_while_a_best_effort_group_never_blocks() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();

            foreach (var node in nodes) {
                var table = node.Host.Services.GetRequiredService<ShardAssignmentTable>();
                var dlMigrationId = new MigrationId(Guid.NewGuid());
                table.Apply(new CreateStream($"{StreamName}.__deadletter__", 1, RoutingAlgorithm.XxHash3V1));
                table.Apply(new MigrationStarted(dlMigrationId, $"{StreamName}.__deadletter__", 0, GroupId, GroupId));
                table.Apply(new Cutover(dlMigrationId, $"{StreamName}.__deadletter__", 0, GroupId));
                table.Apply(new SetStreamRetentionPolicy(StreamName, AuditRetention: TimeSpan.Zero, MaxRetention: TimeSpan.FromMilliseconds(100), MaxSizeBytesPerVirtualShard: null));
                table.Apply(new SetConsumerGroupMandatory(StreamName, "best-effort-group", false));
            }

            using var connection = Connect(nodes);
            await PublishOnceReadyAsync(connection, StreamName, "key-1", "hello"u8.ToArray());

            // Registers both groups (never acking) so this proves the mandatory/best-effort
            // distinction itself, not just "nobody is consuming at all".
            using var mandatoryCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await foreach (var _ in connection.ConsumeAsync(StreamName, "mandatory-group", mandatoryCts.Token)) break;
            using var bestEffortCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await foreach (var _ in connection.ConsumeAsync(StreamName, "best-effort-group", bestEffortCts.Token)) break;

            var leader = nodes.First(node => node.Replica.IsLeader);
            (await RunUntilAsync(() => leader.RetentionEvaluator.DeadLetterMessagesTotal == 1, TimeSpan.FromSeconds(5)))
                .Should().BeTrue("past max-retention and unacked by the mandatory group — must be force-forwarded regardless of the best-effort group's own missing ack");

            // The dead-lettered copy — this harness routes the dead-letter stream onto the same
            // physical group, so it lands as the next offset in the very same ShardLog.
            leader.ShardLog.TryGet(1, out var handle).Should().BeTrue();
            using (handle)
                Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be("hello");
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task Size_based_emergency_pruning_deletes_the_oldest_segment_once_the_ceiling_is_exceeded() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot, flushThresholdBytes: 1); // every publish flushes into its own segment
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();
            var leader = nodes.First(node => node.Replica.IsLeader);

            using var connection = Connect(nodes);
            var first = await PublishOnceReadyAsync(connection, StreamName, "key-1", "one"u8.ToArray());
            await connection.PublishAsync(StreamName, "key-2", "two"u8.ToArray());
            await connection.PublishAsync(StreamName, "key-3", "three"u8.ToArray());

            (await RunUntilAsync(() => leader.ShardLog.TryGet(2, out var handle) && Dispose(handle), TimeSpan.FromSeconds(5)))
                .Should().BeTrue();
            // TryGet succeeding only proves the entry is visible (MemTable or segment) — the
            // size-based mechanism only ever sees flushed *.sst files, so wait for at least one
            // real flush before sizing the ceiling off it. The compaction policy above merges
            // aggressively (MaxMergedSegmentBytes: null), so this may already be a single,
            // combined segment rather than three separate ones — either is fine below.
            (await RunUntilAsync(() => Directory.GetFiles(leader.DataDirectory, "*.sst").Length >= 1, TimeSpan.FromSeconds(5)))
                .Should().BeTrue("flushThresholdBytes: 1 forces at least the first entry to flush almost immediately");

            // Sized off the real, current directory size, not a guessed constant — small enough
            // that at least the oldest offset must go.
            var currentTotalBytes = Directory.GetFiles(leader.DataDirectory, "*.sst").Sum(path => new FileInfo(path).Length);
            var ceiling = currentTotalBytes / 2;

            foreach (var node in nodes) {
                var table = node.Host.Services.GetRequiredService<ShardAssignmentTable>();
                table.Apply(new SetStreamRetentionPolicy(StreamName, AuditRetention: TimeSpan.MaxValue, MaxRetention: TimeSpan.MaxValue, MaxSizeBytesPerVirtualShard: ceiling));
            }

            (await RunUntilAsync(() => !leader.ShardLog.TryGet((ulong)first.Offset, out _), TimeSpan.FromSeconds(10)))
                .Should().BeTrue("the oldest offset must be force-pruned once the directory exceeds its size ceiling, regardless of ack status");
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }

    [Fact]
    public async Task A_leader_crash_never_redelivers_a_checkpoint_mode_ack_that_was_already_committed() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();
            SetCheckpointMode(nodes, "checkpoint-group");
            foreach (var node in nodes)
                node.Host.Services.GetRequiredService<ShardAssignmentTable>().Apply(new SetStreamCheckpointInterval(StreamName, Interval: null, OffsetCount: 1));

            using var connection = Connect(nodes);
            var first = await PublishOnceReadyAsync(connection, StreamName, "key-1", "one"u8.ToArray());
            await connection.AckAsync(StreamName, "checkpoint-group", first.ShardId, first.Offset);

            var leaderNode = nodes.First(node => node.Replica.IsLeader);
            var survivors = nodes.Where(node => node != leaderNode).ToArray();

            // Wait for a real AckCheckpoint to commit and replay into every surviving node's own
            // AckIndex — CommittedWatermark, not LowWatermark, proves it actually went through Raft.
            (await RunUntilAsync(() => survivors.All(node => node.AckIndex.GetOrCreate("checkpoint-group").CommittedWatermark == 1), TimeSpan.FromSeconds(10)))
                .Should().BeTrue("the checkpoint must have committed to every surviving node before the leader crash, not just a quorum majority that might not include them");

            nodes.Remove(leaderNode);
            await leaderNode.DisposeAsync();

            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10)))
                .Should().BeTrue("the two surviving nodes must elect a new leader");

            var messages = new List<RemontoireMessage>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try {
                await foreach (var message in connection.ConsumeAsync(StreamName, "checkpoint-group", cts.Token))
                    messages.Add(message);
            } catch (OperationCanceledException) {
                // Expected — nothing left to redeliver, the stream just idles until the timeout.
            }

            messages.Should().BeEmpty("the ack was already committed via a real AckCheckpoint before the crash — it must never be redelivered");
        } finally {
            await DisposeAllAsync(nodes, directoryRoot);
        }
    }
}
