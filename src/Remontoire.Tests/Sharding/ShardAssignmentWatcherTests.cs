using System.Net;
using FluentAssertions;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire.Meta.V1;
using Remontoire.Raft;
using Remontoire.Server;
using Remontoire.Server.Grpc;
using Remontoire.Storage;

namespace Remontoire.Sharding;

// Layer 3/4: a real Kestrel host serving ShardAssignmentMetaGrpcService, backed by a real
// single-node "meta" RaftReplica + MetaLogJournal + ShardAssignmentTableApplier — exactly what a
// meta-group-hosting server node composes — with a real ShardAssignmentWatcher on the other end
// of a real gRPC connection. Nothing mocked but the process boundary, the same tradeoff every
// other layer-4 test in this codebase already makes.
[Collection("RealNetwork")]
public class ShardAssignmentWatcherTests {
    [Fact]
    public async Task Fills_its_table_from_an_initial_GetSnapshot_and_then_a_live_Watch() {
        var (replica, host, applier) = await ComposeAsync();
        try {
            await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream("orders", 1024, RoutingAlgorithm.XxHash3V1))));

            using var channel = GrpcChannel.ForAddress(host.Urls.First());
            var client = new ShardAssignmentMeta.ShardAssignmentMetaClient(channel);
            var watcherTable = new ShardAssignmentTable();
            await using var watcher = new ShardAssignmentWatcher(client, watcherTable);

            (await WaitUntilAsync(() => watcherTable.TryGetStreamConfig("orders", out _))).Should().BeTrue();

            // A second command, committed AFTER the watcher already started — proves the live
            // Watch tail works, not only the initial GetSnapshot fill.
            await replica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new RegisterGroup("group-1", []))));

            (await WaitUntilAsync(() => watcherTable.TryGetGroup("group-1", out _))).Should().BeTrue();
        } finally {
            await applier.DisposeAsync();
            await host.DisposeAsync();
        }
    }

    static async Task<(RaftReplica Replica, WebApplication Host, ShardAssignmentTableApplier Applier)> ComposeAsync() {
        var config = new RaftReplicaConfig(
            GroupId: "__meta__", NodeId: "node-1", Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync();
        replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node group -> ready leader
        await replica.DrainAsync();

        var table = new ShardAssignmentTable();
        var journal = new MetaLogJournal();
        var applier = new ShardAssignmentTableApplier(replica, table, journal);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc();
        builder.Services.AddSingleton(journal);

        var host = builder.Build();
        host.MapGrpcService<ShardAssignmentMetaGrpcService>();
        await host.StartAsync();

        return (replica, host, applier);
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
