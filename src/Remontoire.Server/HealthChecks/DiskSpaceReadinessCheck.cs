using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Remontoire.Server.HealthChecks;

/// <summary>
/// Fails once any hosted group's (or the meta-group's) data directory has less than
/// <see cref="RaftServerOptions.MinFreeDiskSpaceBytes"/> of free space on its own drive — an acute
/// disk-space shortage is a readiness concern, not a liveness one. Deduplicates by resolved drive
/// root, so two groups sharing one physical disk are only ever checked once per scrape.
/// </summary>
sealed class DiskSpaceReadinessCheck(IOptions<RaftServerOptions> options) : IHealthCheck {
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        var raftOptions = options.Value;
        var dataDirectories = raftOptions.Groups.Select(group => group.DataDirectory);
        if (raftOptions.MetaGroup is { } metaGroup)
            dataDirectories = dataDirectories.Append(metaGroup.DataDirectory);

        var driveRoots = dataDirectories.Select(Path.GetPathRoot).Where(root => !string.IsNullOrEmpty(root)).Distinct();

        foreach (var root in driveRoots) {
            var drive = new DriveInfo(root!);
            if (drive.AvailableFreeSpace < raftOptions.MinFreeDiskSpaceBytes)
                return Task.FromResult(HealthCheckResult.Unhealthy($"{root}: free disk space ({drive.AvailableFreeSpace} bytes) below threshold ({raftOptions.MinFreeDiskSpaceBytes} bytes)."));
        }

        return Task.FromResult(HealthCheckResult.Healthy());
    }
}
