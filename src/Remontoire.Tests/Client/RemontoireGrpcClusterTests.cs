using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
// RaftReplicaHostedService builds), with a real RemontoireConnection on top. Nothing is mocked —
// only the process boundary is collapsed, the same tradeoff RaftGrpcClusterTests already makes.
public class RemontoireGrpcClusterTests {
    const string GroupId = "group-1";

    // Must equal GroupId: RemontoireClientGrpcService.Resolve treats stream_name as literally the
    // group_id (the documented fase-4 simplification — no stream_name -> group_id translation
    // exists server-side yet, only RemontoireClientOptions.StreamGroupIds does that client-side,
    // for its own leader-cache bookkeeping; the wire request still carries the raw stream name).
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

    sealed class Node : IAsyncDisposable {
        public required WebApplication Host { get; init; }
        public required RaftReplica Replica { get; init; }
        public required RaftGrpcTransport Transport { get; init; }
        public required ShardLog ShardLog { get; init; }
        public required AckIndex AckIndex { get; init; }
        public required AckIndexApplier Applier { get; init; }

        public async ValueTask DisposeAsync() {
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

        var app = builder.Build();
        app.MapGrpcService<RaftTransportGrpcService>();
        app.MapGrpcService<RemontoireClientGrpcService>();
        await app.StartAsync();
        return app;
    }

    static async Task<List<Node>> StartClusterAsync(string directoryRoot) {
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
            var directory = Path.Combine(directoryRoot, nodeId);
            Directory.CreateDirectory(directory);
            var shardLog = await ShardLog.OpenAsync(directory, replica.ReadCommittedAsync,
                compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null,
                    GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())));
            var applier = new AckIndexApplier(shardLog, ackIndex);
            hosts[i].Services.GetRequiredService<MessagingGroupRegistry>().Register(GroupId, shardLog, ackIndex);

            // No meta-group in this harness — seed each node's table directly with the one
            // stream/group/assignment this whole test file uses, standing in for what a real
            // meta-group commit would otherwise replicate.
            var table = hosts[i].Services.GetRequiredService<ShardAssignmentTable>();
            table.Apply(new CreateStream(StreamName, VirtualShardCount: 1, RoutingAlgorithm.XxHash3V1));
            table.Apply(new RegisterGroup(GroupId, members.Select(member => new ShardGroupMember(member.NodeId, member.Address)).ToArray()));
            table.Apply(new Cutover(MigrationId: "seed", StreamName, VirtualShardIndex: 0, GroupId));

            nodes.Add(new Node { Host = hosts[i], Replica = replica, Transport = transport, ShardLog = shardLog, AckIndex = ackIndex, Applier = applier });
        }

        return nodes;
    }

    static RemontoireConnection Connect(IEnumerable<Node> nodes) =>
        new(new RemontoireClientOptions(
            StreamGroupIds: new Dictionary<string, string> { [StreamName] = GroupId },
            GroupMemberAddresses: nodes.Select(node => new Uri(node.Host.Urls.First())).ToArray(),
            MaxRedirectAttempts: 10,
            RedirectRetryDelay: TimeSpan.FromMilliseconds(50)));

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
            var result = await connection.PublishAsync(StreamName, "key-1", "hello"u8.ToArray());
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
    public async Task PublishAsync_against_a_follower_redirects_transparently_to_the_real_leader() {
        var directoryRoot = CreateTempDirectory();
        var nodes = await StartClusterAsync(directoryRoot);
        try {
            (await RunUntilAsync(() => nodes.Any(node => node.Replica.IsLeader), TimeSpan.FromSeconds(10))).Should().BeTrue();
            var follower = nodes.First(node => !node.Replica.IsLeader);

            using var connection = Connect([follower]); // only knows the follower's address up front

            var result = await connection.PublishAsync(StreamName, "key-1", "hello"u8.ToArray());

            result.Offset.Should().Be(0, "the redirect must reach the real leader even though the connection only knew the follower's address");
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
                await seed.PublishAsync(StreamName, "key-1", "hello"u8.ToArray());

            var follower = nodes.First(node => !node.Replica.IsLeader);
            using var connection = Connect([follower]); // only knows the follower's address up front

            var messages = new List<RemontoireMessage>();
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                await foreach (var message in connection.ConsumeAsync(StreamName, "group-a", cts.Token)) {
                    messages.Add(message);
                    break;
                }

            var expectedPayload = "hello"u8.ToArray();
            messages.Should().ContainSingle(message => message.Payload.ToArray().SequenceEqual(expectedPayload),
                "the stream must redirect to the real leader even though the connection only knew the follower's address");
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
            await connection.PublishAsync(StreamName, "key-1", "one"u8.ToArray());
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
            var first = await connection.PublishAsync(StreamName, "key-1", "one"u8.ToArray());
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
}
