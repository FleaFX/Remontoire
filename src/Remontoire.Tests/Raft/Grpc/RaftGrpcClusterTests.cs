using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire.Raft.Grpc;
using Remontoire.Storage;

namespace Remontoire.Raft;

// Testlaag 4: real gRPC, over real loopback sockets, between real Kestrel hosts — one process,
// three in-process "nodes" each with their own RaftReplicaRegistry/RaftTransportGrpcService and
// their own RaftGrpcTransport client channels to the other two. Everything below layer 3
// (RaftReplica itself) is exactly what a real 3-process deployment runs; only the process
// boundary is collapsed, for automated-test practicality. Timing is real wall-clock, unlike
// SimulatedCluster's virtual time — polling below uses real, generous timeouts.
[Collection("RealNetwork")]
public class RaftGrpcClusterTests {
    static AppendRequest SampleRequest(string key = "key") =>
        new(System.Text.Encoding.UTF8.GetBytes(key), [], "payload"u8.ToArray());

    static async Task<bool> RunUntilAsync(Func<bool> condition, TimeSpan timeout) {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline) {
            if (condition())
                return true;
            await Task.Delay(20);
        }
        return condition();
    }

    static async Task<WebApplication> StartHostAsync() {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(System.Net.IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

        builder.Services.AddGrpc();
        builder.Services.AddSingleton<RaftReplicaRegistry>();

        var app = builder.Build();
        app.MapGrpcService<RaftTransportGrpcService>();
        await app.StartAsync();
        return app;
    }

    [Fact]
    public async Task Three_real_grpc_hosts_elect_a_leader_and_commit_a_proposal() {
        var hosts = await Task.WhenAll(StartHostAsync(), StartHostAsync(), StartHostAsync());
        var replicas = new List<RaftReplica>();
        var transports = new List<RaftGrpcTransport>();

        try {
            var nodeIds = new[] { "node-1", "node-2", "node-3" };
            var members = nodeIds.Zip(hosts, (nodeId, host) => new RaftGroupMember(nodeId, new Uri(host.Urls.First()))).ToArray();

            for (var i = 0; i < hosts.Length; i++) {
                var nodeId = nodeIds[i];
                var peers = members.Where(member => member.NodeId != nodeId).ToArray();
                var config = new RaftReplicaConfig(
                    GroupId: "group-1", NodeId: nodeId, Peers: peers,
                    HeartbeatInterval: TimeSpan.FromMilliseconds(50),
                    ElectionTimeoutMin: TimeSpan.FromMilliseconds(150),
                    ElectionTimeoutMax: TimeSpan.FromMilliseconds(300));

                var transport = new RaftGrpcTransport(peers, config.ResolvedRpcTimeout);
                transports.Add(transport);

                var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), transport, config);
                replicas.Add(replica);

                await replica.StartAsync();
                hosts[i].Services.GetRequiredService<RaftReplicaRegistry>().Register(replica);
            }

            var elected = await RunUntilAsync(() => replicas.Any(replica => replica.IsLeader), TimeSpan.FromSeconds(10));
            elected.Should().BeTrue("three real gRPC-connected nodes with a reliable loopback network must elect a leader");
            replicas.Count(replica => replica.IsLeader).Should().Be(1);

            var leader = replicas.First(replica => replica.IsLeader);
            var proposeTask = leader.ProposeAsync(SampleRequest()).AsTask();

            (await Task.WhenAny(proposeTask, Task.Delay(TimeSpan.FromSeconds(10))))
                .Should().Be(proposeTask, "the proposal must commit over the real transport within the timeout");
            var result = await proposeTask;

            foreach (var replica in replicas)
                (await RunUntilAsync(() => replica.CommitIndex >= result.RaftIndex, TimeSpan.FromSeconds(10)))
                    .Should().BeTrue("every real node must eventually replicate the committed entry over gRPC");
        } finally {
            foreach (var replica in replicas)
                await replica.DisposeAsync();
            foreach (var transport in transports)
                transport.Dispose();
            foreach (var host in hosts)
                await host.DisposeAsync();
        }
    }
}
