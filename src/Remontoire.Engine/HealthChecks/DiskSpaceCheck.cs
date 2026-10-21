using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Remontoire.Engine.HealthChecks;

/// <summary>
/// Fails once the drive of any data directory has less than the threshold of free space — an acute
/// disk-space shortage is a readiness concern, not a liveness one. Checks each drive once, however
/// many directories share it.
/// </summary>
/// <param name="dataDirectories">The data directories to check, read again at every check.</param>
/// <param name="minFreeBytes">The free space below which the check fails.</param>
sealed class DiskSpaceCheck(Func<IEnumerable<string>> dataDirectories, long minFreeBytes) : IHealthCheck {
    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        // GetFullPath first: GetPathRoot returns "" for a relative path, which would otherwise drop it
        // from the roots below and never fail. OrdinalIgnoreCase: Windows drive letters are
        // case-insensitive ("C:\" and "c:\" are the same drive).
        var driveRoots = dataDirectories().Select(directory => Path.GetPathRoot(Path.GetFullPath(directory)))
            .Where(root => !string.IsNullOrEmpty(root)).Distinct(StringComparer.OrdinalIgnoreCase);

        var failures = new List<string>();
        foreach (var root in driveRoots) {
            var drive = new DriveInfo(root!);
            if (drive.AvailableFreeSpace < minFreeBytes)
                failures.Add($"{root}: free disk space ({drive.AvailableFreeSpace} bytes) below threshold ({minFreeBytes} bytes).");
        }

        return Task.FromResult(failures.Count == 0
            ? HealthCheckResult.Healthy()
            : new HealthCheckResult(context.Registration.FailureStatus, string.Join("; ", failures)));
    }
}
