using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Admin.V1;
using Remontoire.Client;
using Remontoire.Server;

namespace Remontoire.Cluster;

// The hosting API's exit criterion: a three-node cluster wired exclusively through the public
// surface (AddRemontoire/AddCluster, MapRemontoire/MapRemontoireCluster/MapRemontoireAdmin,
// AddClient) publishes, consumes and acks over real loopback sockets — plus the wiring rules
// that aren't visible from a running cluster (the meta-group validation and which endpoints
// MapRemontoire maps on which node).
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

    [Fact]
    public async Task A_three_node_cluster_built_with_the_fluent_API_publishes_consumes_and_acks() {
        var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var nodes = new[] { "node-1", "node-2", "node-3" }.Select(nodeId => new Node(nodeId, FreePort(), FreePort())).ToArray();
        var apps = new List<WebApplication>();
        var channels = new List<GrpcChannel>();
        ServiceProvider? provider = null;
        try {
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
                }).AddServer();

                var app = builder.Build();
                app.MapRemontoireCluster().RequireHost($"*:{node.PeerPort}");
                app.MapRemontoire().RequireHost($"*:{node.ClientPort}");
                app.MapRemontoireAdmin().RequireHost($"*:{node.ClientPort}");
                await app.StartAsync();
                apps.Add(app);
            }

            // Stream creation goes through the generated admin client of Remontoire.Grpc, since
            // Remontoire.Client has no admin API yet. Whichever node currently leads the meta
            // group accepts; every other one answers NotLeader (or is still electing, as an RpcException).
            channels.AddRange(nodes.Select(node => GrpcChannel.ForAddress(node.ClientAddress)));
            var adminClients = channels.Select(channel => new RemontoireAdmin.RemontoireAdminClient(channel)).ToArray();
            var bootstrapRequest = new CreateStreamRequest {
                StreamName = StreamName,
                VirtualShardCount = 1,
                RoutingAlgorithm = RoutingAlgorithmProto.XxHash3V1,
                InitialGroups = { new PhysicalGroupDescriptorProto {
                    GroupId = GroupId,
                    Members = { nodes.Select(node => new ShardGroupMemberProto { NodeId = node.NodeId, Address = node.ClientAddress }) },
                } },
                InitialAssignments = { new VirtualShardAssignmentProto { VirtualShardIndex = 0, GroupId = GroupId } },
            };
            var created = false;
            var createDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
            while (!created && DateTime.UtcNow < createDeadline) {
                foreach (var adminClient in adminClients) {
                    try {
                        var reply = await adminClient.CreateStreamAsync(bootstrapRequest);
                        if (reply.ResultCase == CreateStreamReply.ResultOneofCase.Success) {
                            created = true;
                            break;
                        }
                    } catch (RpcException) {
                        // Not the meta-group leader's answer, or not reachable yet — try the next node.
                    }
                }
                if (!created)
                    await Task.Delay(100);
            }
            created.Should().BeTrue("CreateStream must succeed on the meta-group leader within 20 seconds");

            var services = new ServiceCollection();
            services.AddRemontoire().AddClient(o => {
                o.MetaGroupSeedAddresses = nodes.Select(node => new Uri(node.ClientAddress)).ToList();
                o.MaxRedirectAttempts = 20;
                o.RedirectRetryDelay = TimeSpan.FromMilliseconds(50);
            });
            provider = services.BuildServiceProvider();
            var producer = provider.GetRequiredService<IRemontoireProducer>();
            var consumer = provider.GetRequiredService<IRemontoireConsumer>();

            // The fresh connection's watcher may still be catching up on the stream just created —
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
            foreach (var channel in channels)
                channel.Dispose();
            // Stop before disposing: disposing a host doesn't stop its hosted services, so the WAL
            // files would stay open (and undeletable on Windows).
            foreach (var app in apps) {
                await app.StopAsync();
                await app.DisposeAsync();
            }
            Directory.Delete(root, recursive: true);
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
    public void MapRemontoire_maps_the_assignment_feed_on_a_node_that_hosts_a_meta_group_replica() {
        var dataDirectory = CreateTempDirectory();
        var metaDirectory = CreateTempDirectory();
        try {
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddRemontoire().AddCluster(o => {
                o.Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }];
                o.MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory };
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
