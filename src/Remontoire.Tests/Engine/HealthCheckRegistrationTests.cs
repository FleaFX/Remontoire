using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Remontoire;

namespace Remontoire.Engine;

// The host owns the registration: names, failure statuses and tags are the host's choice, and
// these pin what the extensions register when the host passes nothing — plus the readiness
// lifecycle of a real embedded node, from before its start until after its stop.
public class HealthCheckRegistrationTests {
    [Fact]
    public void Registers_the_three_checks_with_their_default_names_and_tags() {
        var services = new ServiceCollection();
        services.AddHealthChecks().AddRemontoireLiveness().AddRemontoireReadiness().AddRemontoireDiskSpace();

        var registrations = services.BuildServiceProvider()
            .GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;

        registrations.Should().HaveCount(3);
        registrations.Should().ContainSingle(r => r.Name == "remontoire-liveness").Which.Tags.Should().Equal("live");
        registrations.Should().ContainSingle(r => r.Name == "remontoire-readiness").Which.Tags.Should().Equal("ready");
        registrations.Should().ContainSingle(r => r.Name == "remontoire-disk-space").Which.Tags.Should().Equal("ready");
        registrations.Should().OnlyContain(r => r.FailureStatus == HealthStatus.Unhealthy);
    }

    [Fact]
    public void Takes_the_name_failure_status_and_tags_the_host_passes() {
        var services = new ServiceCollection();
        services.AddHealthChecks()
            .AddRemontoireReadiness("ready-check", HealthStatus.Degraded, ["k8s"])
            .AddRemontoireLiveness(tags: []);

        var registrations = services.BuildServiceProvider()
            .GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;

        var readiness = registrations.Should().ContainSingle(r => r.Name == "ready-check").Which;
        readiness.FailureStatus.Should().Be(HealthStatus.Degraded);
        readiness.Tags.Should().Equal("k8s");
        registrations.Should().ContainSingle(r => r.Name == "remontoire-liveness").Which.Tags.Should().BeEmpty();
    }

    [Fact]
    public async Task An_embedded_node_is_ready_after_it_started_and_unready_after_it_stopped() {
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        IHost? host = null;
        try {
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddRemontoire().AddEmbedded(options => options.DataDirectory = dataDirectory);
            builder.Services.AddHealthChecks()
                .AddRemontoireLiveness()
                .AddRemontoireReadiness()
                .AddRemontoireDiskSpace(minFreeBytes: 0);
            host = builder.Build();
            var service = host.Services.GetRequiredService<HealthCheckService>();

            (await StatusAsync(service, "remontoire-readiness")).Should().Be(HealthStatus.Unhealthy, "the node has not started");
            (await StatusAsync(service, "remontoire-liveness")).Should().Be(HealthStatus.Healthy, "nothing is stuck");
            (await StatusAsync(service, "remontoire-disk-space")).Should().Be(HealthStatus.Healthy, "no directory is open yet");

            await host.StartAsync();
            await host.Services.GetRequiredService<IRemontoireAdmin>().EnsureStreamAsync("health");

            (await StatusAsync(service, "remontoire-readiness")).Should().Be(HealthStatus.Healthy);
            (await StatusAsync(service, "remontoire-liveness")).Should().Be(HealthStatus.Healthy);
            (await StatusAsync(service, "remontoire-disk-space")).Should().Be(HealthStatus.Healthy);
            var node = host.Services.GetRequiredService<RemontoireNode>();
            node.DataDirectories.Should().Contain(Path.Combine(dataDirectory, "meta"));
            node.DataDirectories.Should().Contain(Path.Combine(dataDirectory, "groups", "health"));

            await host.StopAsync();

            (await StatusAsync(service, "remontoire-readiness")).Should().Be(HealthStatus.Unhealthy, "the node has stopped");
            host.Dispose();
            host = null;
        } finally {
            if (host is not null) {
                await host.StopAsync();
                host.Dispose();
            }
            Directory.Delete(dataDirectory, recursive: true);
        }
    }

    static async Task<HealthStatus> StatusAsync(HealthCheckService service, string name) {
        var report = await service.CheckHealthAsync(registration => registration.Name == name, CancellationToken.None);
        return report.Entries[name].Status;
    }
}
