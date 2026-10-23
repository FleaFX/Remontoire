using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Engine;

namespace Remontoire.Cluster;

// Readiness across the cluster's own hosting path: a generic host (no Kestrel, no network — the
// group has no peers) whose node becomes ready once its group elects itself leader, and unready
// again the moment the host stops it.
public class ClusterHealthCheckTests {
    [Fact]
    public async Task A_cluster_node_is_ready_once_its_group_leads_and_unready_after_it_stopped() {
        var groupDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var metaDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        IHost? host = null;
        try {
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddRemontoire().AddCluster(o => {
                o.Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = groupDirectory }];
                o.MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory };
                o.ClientAddress = "http://127.0.0.1:1";
            });
            builder.Services.AddHealthChecks().AddRemontoireReadiness();
            host = builder.Build();
            var service = host.Services.GetRequiredService<HealthCheckService>();

            await host.StartAsync();

            (await ConditionPoller.WaitUntilAsync(() =>
                    service.CheckHealthAsync(CancellationToken.None).GetAwaiter().GetResult().Status == HealthStatus.Healthy,
                TimeSpan.FromSeconds(10))).Should().BeTrue("a peerless group elects its leader within 250-500 ms, so readiness must follow");

            await host.StopAsync();

            (await service.CheckHealthAsync(CancellationToken.None)).Status.Should().Be(HealthStatus.Unhealthy,
                "the node has started stopping — its readiness must fail again");
            host.Dispose();
            host = null;
        } finally {
            if (host is not null) {
                await host.StopAsync();
                host.Dispose();
            }
            Directory.Delete(groupDirectory, recursive: true);
            Directory.Delete(metaDirectory, recursive: true);
        }
    }
}
