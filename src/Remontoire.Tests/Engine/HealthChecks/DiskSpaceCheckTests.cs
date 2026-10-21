using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Remontoire.Engine.HealthChecks;

public class DiskSpaceCheckTests {
    [Fact]
    public async Task Healthy_when_free_space_is_above_the_threshold() {
        var check = new DiskSpaceCheck(() => [Path.GetTempPath()], 1);

        (await check.CheckHealthAsync(Context(check))).Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Unhealthy_when_free_space_is_below_the_threshold() {
        var check = new DiskSpaceCheck(() => [Path.GetTempPath()], long.MaxValue);

        var result = await check.CheckHealthAsync(Context(check));

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("below threshold");
    }

    // Regression test: Path.GetPathRoot returns "" for a relative path (e.g. the cluster sample
    // configures DataDirectory as a relative "data/node-1/group-1"), which used to get
    // silently filtered out of driveRoots entirely — the check would then unconditionally report
    // Healthy regardless of actual free space, never even reaching the DriveInfo lookup.
    [Fact]
    public async Task Resolves_a_relative_DataDirectory_to_its_real_drive_instead_of_silently_skipping_it() {
        var check = new DiskSpaceCheck(() => ["some-relative-directory"], long.MaxValue);

        var result = await check.CheckHealthAsync(Context(check));

        result.Status.Should().Be(HealthStatus.Unhealthy, "a relative path must still resolve to its real drive, not be silently dropped from the check");
    }

    [Fact]
    public async Task Checks_every_directory_it_is_given() {
        var check = new DiskSpaceCheck(() => [Path.GetTempPath(), Path.GetTempPath()], long.MaxValue);

        (await check.CheckHealthAsync(Context(check))).Status.Should().Be(HealthStatus.Unhealthy);
    }

    static HealthCheckContext Context(IHealthCheck check) =>
        new() { Registration = new HealthCheckRegistration("test", check, HealthStatus.Unhealthy, null) };
}
