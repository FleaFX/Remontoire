using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Remontoire.Server.HealthChecks;

public class DiskSpaceReadinessCheckTests {
    [Fact]
    public async Task Healthy_when_free_space_is_above_the_threshold() {
        var check = new DiskSpaceReadinessCheck(OptionsFor(Path.GetTempPath(), minFreeDiskSpaceBytes: 1));

        (await check.CheckHealthAsync(new HealthCheckContext())).Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Unhealthy_when_free_space_is_below_the_threshold() {
        var check = new DiskSpaceReadinessCheck(OptionsFor(Path.GetTempPath(), minFreeDiskSpaceBytes: long.MaxValue));

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("below threshold");
    }

    // Regression test: Path.GetPathRoot returns "" for a relative path (e.g. this repo's own
    // appsettings.json configures DataDirectory as a relative "data/node-1"), which used to get
    // silently filtered out of driveRoots entirely — the check would then unconditionally report
    // Healthy regardless of actual free space, never even reaching the DriveInfo lookup.
    [Fact]
    public async Task Resolves_a_relative_DataDirectory_to_its_real_drive_instead_of_silently_skipping_it() {
        var check = new DiskSpaceReadinessCheck(OptionsFor("some-relative-directory", minFreeDiskSpaceBytes: long.MaxValue));

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.Should().Be(HealthStatus.Unhealthy, "a relative path must still resolve to its real drive, not be silently dropped from the check");
    }

    [Fact]
    public async Task Checks_both_the_meta_group_and_every_data_group_s_directory() {
        var options = new RaftServerOptions {
            Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = Path.GetTempPath() }],
            MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = Path.GetTempPath() },
            MinFreeDiskSpaceBytes = long.MaxValue,
        };
        var check = new DiskSpaceReadinessCheck(Options.Create(options));

        (await check.CheckHealthAsync(new HealthCheckContext())).Status.Should().Be(HealthStatus.Unhealthy);
    }

    static IOptions<RaftServerOptions> OptionsFor(string dataDirectory, long minFreeDiskSpaceBytes) =>
        Options.Create(new RaftServerOptions {
            Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
            MinFreeDiskSpaceBytes = minFreeDiskSpaceBytes,
        });
}
