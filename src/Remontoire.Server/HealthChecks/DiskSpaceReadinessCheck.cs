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

        // GetFullPath first: GetPathRoot returns "" for a relative path (this repo's own
        // appsettings.json configures DataDirectory as a relative "data/node-1"), which would
        // otherwise silently drop it from driveRoots below and this check would never fail no
        // matter how little free space is left. OrdinalIgnoreCase for Distinct: Windows drive
        // letters are case-insensitive ("C:\" and "c:\" are the same drive).
        var driveRoots = dataDirectories.Select(directory => Path.GetPathRoot(Path.GetFullPath(directory)))
            .Where(root => !string.IsNullOrEmpty(root)).Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (var root in driveRoots) {
            var drive = new DriveInfo(root!);
            if (drive.AvailableFreeSpace < raftOptions.MinFreeDiskSpaceBytes)
                return Task.FromResult(HealthCheckResult.Unhealthy($"{root}: free disk space ({drive.AvailableFreeSpace} bytes) below threshold ({raftOptions.MinFreeDiskSpaceBytes} bytes)."));
        }

        return Task.FromResult(HealthCheckResult.Healthy());
    }
}
