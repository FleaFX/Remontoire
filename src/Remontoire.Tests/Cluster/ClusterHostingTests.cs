using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Client;
using Remontoire.Engine;
using Remontoire.Raft.Grpc;
using Remontoire.Server;
using Remontoire.Sharding;

namespace Remontoire.Cluster;

// The hosting API's exit criterion: a three-node cluster wired exclusively through the public
// surface (AddRemontoire/AddCluster, MapRemontoire/MapRemontoireCluster/MapRemontoireAdmin,
// AddClient) publishes, consumes and acks over real loopback sockets — the client declares the
// stream and the consumer group through the admin endpoint — plus the wiring rules that aren't
// visible from a running cluster (the meta-group validation and which endpoints MapRemontoire
// maps on which node).
[Collection("RealNetwork")]
public class ClusterHostingTests {
    const string GroupId = "group-1";
    const string StreamName = "orders";

    sealed record Node(string NodeId, int PeerPort, int ClientPort) {
        public string PeerAddress => $"http://127.0.0.1:{PeerPort}";
        public string ClientAddress => $"http://127.0.0.1:{ClientPort}";
    }

    static int FreePort() {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        } finally {
            listener.Stop();
        }
    }

    static string CreateTempDirectory() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }

    static List<string> RoutePatterns(IEndpointRouteBuilder endpoints) =>
        endpoints.DataSources.SelectMany(source => source.Endpoints).OfType<RouteEndpoint>().Select(endpoint => endpoint.RoutePattern.RawText).OfType<string>().ToList();

    static async Task<(Node[] Nodes, List<WebApplication> Apps)> StartClusterAsync(string root) {
        var nodes = new[] { "node-1", "node-2", "node-3" }.Select(nodeId => new Node(nodeId, FreePort(), FreePort())).ToArray();
        var apps = new List<WebApplication>();
        foreach (var node in nodes) {
            var peers = nodes.Where(other => other.NodeId != node.NodeId)
                .Select(other => new RaftPeerOptions { NodeId = other.NodeId, Address = other.PeerAddress, ClientAddress = other.ClientAddress })
                .ToList();

            Directory.CreateDirectory(Path.Combine(root, node.NodeId, GroupId));
            Directory.CreateDirectory(Path.Combine(root, node.NodeId, "meta"));

            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(options => {
                options.Listen(IPAddress.Loopback, node.PeerPort, listen => listen.Protocols = HttpProtocols.Http2);
                options.Listen(IPAddress.Loopback, node.ClientPort, listen => listen.Protocols = HttpProtocols.Http2);
            });
            builder.Services.AddRemontoire().AddCluster(o => {
                o.Groups = [new RaftGroupOptions {
                    GroupId = GroupId, NodeId = node.NodeId, DataDirectory = Path.Combine(root, node.NodeId, GroupId), Peers = peers,
                }];
                o.MetaGroup = new MetaGroupOptions { NodeId = node.NodeId, DataDirectory = Path.Combine(root, node.NodeId, "meta"), Peers = peers };
                o.ClientAddress = node.ClientAddress;
            }).AddServer();

            var app = builder.Build();
            app.MapRemontoireCluster().RequireHost($"*:{node.PeerPort}");
            app.MapRemontoire().RequireHost($"*:{node.ClientPort}");
            app.MapRemontoireAdmin().RequireHost($"*:{node.ClientPort}");
            await app.StartAsync();
            apps.Add(app);
        }

        return (nodes, apps);
    }

    // The data groups register themselves in the meta log on the background once their nodes
    // start, so a declaration right after the cluster is up may not find any group yet.
    static async Task RetryUntilAvailableAsync(Func<Task> declare) {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (true) {
            try {
                await declare();
                return;
            } catch (RemontoireUnavailableException) when (DateTime.UtcNow < deadline) {
                await Task.Delay(100);
            }
        }
    }

    static async Task StopAsync(List<WebApplication> apps, string root) {
        // Stop before disposing: disposing a host doesn't stop its hosted services, so the WAL
        // files would stay open (and undeletable on Windows).
        foreach (var app in apps) {
            await app.StopAsync();
            await app.DisposeAsync();
        }
        Directory.Delete(root, recursive: true);
    }

    [Fact]
    public async Task A_three_node_cluster_built_with_the_fluent_API_publishes_consumes_and_acks() {
        var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var apps = new List<WebApplication>();
        ServiceProvider? provider = null;
        try {
            var (nodes, startedApps) = await StartClusterAsync(root);
            apps = startedApps;

            var services = new ServiceCollection();
            services.AddRemontoire().AddClient(o => {
                o.MetaGroupSeedAddresses = nodes.Select(node => new Uri(node.ClientAddress)).ToList();
                o.MaxRedirectAttempts = 20;
                o.RedirectRetryDelay = TimeSpan.FromMilliseconds(50);
            });
            provider = services.BuildServiceProvider();
            var producer = provider.GetRequiredService<IRemontoireProducer>();
            var consumer = provider.GetRequiredService<IRemontoireConsumer>();
            var admin = provider.GetRequiredService<IRemontoireAdmin>();
            await RetryUntilAvailableAsync(() => admin.EnsureStreamAsync(StreamName));
            await RetryUntilAvailableAsync(() => admin.EnsureConsumerGroupAsync(StreamName, "billing"));

            (await admin.GetStreamAsync(StreamName))!.ConsumerGroups.Select(group => group.Name).Should().Equal("billing");
            var firstTable = apps[0].Services.GetRequiredService<ShardAssignmentTable>();
            firstTable.TryGetGroup(GroupId, out var group).Should().BeTrue();
            group.Members.Should().BeEquivalentTo(nodes.Select(node => new ShardGroupMember(node.NodeId, new Uri(node.ClientAddress))));

            // The fresh connection's watcher may still be catching up on the stream just declared —
            // the same race RemontoireGrpcClusterTests's own publish retry anticipates.
            var publishDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            PublishResult? result = null;
            while (result is null) {
                try {
                    result = await producer.PublishAsync(StreamName, "key-1", "hello"u8.ToArray());
                } catch (Exception ex) when (ex is ArgumentException or RemontoireUnavailableException && DateTime.UtcNow < publishDeadline) {
                    await Task.Delay(20);
                }
            }

            RemontoireMessage? message = null;
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                await foreach (var candidate in consumer.ConsumeAsync(StreamName, "billing", cts.Token)) {
                    message = candidate;
                    break;
                }
            message.Should().NotBeNull();
            message!.Payload.ToArray().Should().Equal("hello"u8.ToArray());
            message.PartitionKey.Should().Be("key-1");
            message.Offset.Should().Be(result.Offset);

            await consumer.AckAsync(StreamName, "billing", message.ShardId, message.Offset);
        } finally {
            if (provider is not null)
                await provider.DisposeAsync();
            await StopAsync(apps, root);
        }
    }

    [Fact]
    public async Task A_follower_node_declares_in_process_through_the_meta_leader() {
        var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var apps = new List<WebApplication>();
        try {
            apps = (await StartClusterAsync(root)).Apps;

            WebApplication? follower = null;
            var hasLeader = false;
            var leaderDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while ((!hasLeader || follower is null) && DateTime.UtcNow < leaderDeadline) {
                hasLeader = false;
                foreach (var app in apps) {
                    var registry = app.Services.GetRequiredService<RaftReplicaRegistry>();
                    if (!registry.TryGet(RemontoireNode.MetaGroupId, out var replica))
                        continue;
                    if (replica.IsLeader)
                        hasLeader = true;
                    else
                        follower = app;
                }
                if (!hasLeader || follower is null)
                    await Task.Delay(50);
            }
            hasLeader.Should().BeTrue("a three-node meta group elects a leader");
            follower.Should().NotBeNull("a three-node meta group has two followers");

            var admin = follower!.Services.GetRequiredService<IRemontoireAdmin>();
            await RetryUntilAvailableAsync(() => admin.EnsureStreamAsync("payments"));

            foreach (var app in apps) {
                var table = app.Services.GetRequiredService<ShardAssignmentTable>();
                (await Task.Run(() => ConditionPoller.WaitUntilAsync(() => table.TryGetAssignment("payments", 0, out _),
                    TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(20)))).Should().BeTrue("every node applies the meta log, whatever it proposed");
            }
        } finally {
            await StopAsync(apps, root);
        }
    }

    [Fact]
    public void AddCluster_throws_when_the_node_neither_hosts_a_meta_group_nor_knows_a_seed() {
        var act = () => { new ServiceCollection().AddRemontoire().AddCluster(o => o.Groups = [new RaftGroupOptions { GroupId = "group-1" }]); };

        act.Should().Throw<ArgumentException>().Which.Message.Should()
            .Contain(nameof(RemontoireClusterOptions.MetaGroup))
            .And.Contain(nameof(RemontoireClusterOptions.MetaGroupSeedAddresses));
    }

    [Fact]
    public void AddCluster_requires_a_client_address_on_a_meta_member_that_hosts_groups() {
        var dataDirectory = CreateTempDirectory();
        var metaDirectory = CreateTempDirectory();
        try {
            var act = () => {
                new ServiceCollection().AddRemontoire().AddCluster(o => {
                    o.Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }];
                    o.MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory };
                });
            };

            act.Should().Throw<ArgumentException>().Which.Message.Should().Contain(nameof(RemontoireClusterOptions.ClientAddress));
        } finally {
            Directory.Delete(dataDirectory, recursive: true);
            Directory.Delete(metaDirectory, recursive: true);
        }
    }

    [Fact]
    public void MapRemontoire_maps_the_assignment_feed_on_a_node_that_hosts_a_meta_group_replica() {
        var dataDirectory = CreateTempDirectory();
        var metaDirectory = CreateTempDirectory();
        try {
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddRemontoire().AddCluster(o => {
                o.Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }];
                o.MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory };
                o.ClientAddress = "http://127.0.0.1:1";
            });
            var app = builder.Build();
            app.MapRemontoire();

            var patterns = RoutePatterns((IEndpointRouteBuilder)app);

            patterns.Should().Contain(pattern => pattern.StartsWith("/remontoire.client.v1.RemontoireClient/"));
            patterns.Should().Contain(pattern => pattern.StartsWith("/remontoire.meta.v1.ShardAssignmentMeta/"));
        } finally {
            Directory.Delete(dataDirectory, recursive: true);
            Directory.Delete(metaDirectory, recursive: true);
        }
    }

    [Fact]
    public void MapRemontoire_does_not_map_the_assignment_feed_on_a_node_without_a_meta_group_replica() {
        var dataDirectory = CreateTempDirectory();
        try {
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddRemontoire().AddCluster(o => {
                o.Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }];
                o.MetaGroupSeedAddresses = ["http://127.0.0.1:1"];
            });
            var app = builder.Build();
            app.MapRemontoire();

            var patterns = RoutePatterns((IEndpointRouteBuilder)app);

            patterns.Should().Contain(pattern => pattern.StartsWith("/remontoire.client.v1.RemontoireClient/"));
            patterns.Should().NotContain(pattern => pattern.StartsWith("/remontoire.meta.v1.ShardAssignmentMeta/"));
        } finally {
            Directory.Delete(dataDirectory, recursive: true);
        }
    }
}
