using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Remontoire.Engine.HealthChecks;
using Remontoire.Raft.Grpc;

namespace Remontoire.Engine;

/// <summary>
/// Adds the health checks of a Remontoire node, embedded or a cluster node, to the host's health
/// checks. Remontoire maps no endpoint and writes no response: the host maps its own and selects the
/// checks by tag.
/// </summary>
public static class RemontoireHealthChecksBuilderExtensions {
    /// <summary>
    /// The default free-space threshold of <see cref="AddRemontoireDiskSpace"/>: 1 GiB.
    /// </summary>
    public const long DefaultMinFreeDiskSpaceBytes = 1L << 30;

    /// <summary>
    /// Adds a liveness check: it fails when the actor loop of a hosted replica, or a WAL flush, has
    /// stalled for more than five seconds, and never because a group has no leader.
    /// </summary>
    /// <param name="builder">The host's health-checks builder.</param>
    /// <param name="name">The check's name.</param>
    /// <param name="failureStatus">The status reported on failure; <see langword="null"/> means <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">The check's tags; <see langword="null"/> means <c>live</c>.</param>
    /// <returns>The same <paramref name="builder"/> for further chaining.</returns>
    public static IHealthChecksBuilder AddRemontoireLiveness(
        this IHealthChecksBuilder builder,
        string name = "remontoire-liveness",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null
    ) {
        return builder.Add(new HealthCheckRegistration(name,
            provider => new LivenessCheck(provider.GetRequiredService<RaftReplicaRegistry>()),
            failureStatus, tags ?? ["live"]));
    }

    /// <summary>
    /// Adds a readiness check: it passes once the node has started — an embedded node once every
    /// stream has caught up — and as long as at least one hosted group has a leader it can reach.
    /// </summary>
    /// <param name="builder">The host's health-checks builder.</param>
    /// <param name="name">The check's name.</param>
    /// <param name="failureStatus">The status reported on failure; <see langword="null"/> means <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">The check's tags; <see langword="null"/> means <c>ready</c>.</param>
    /// <returns>The same <paramref name="builder"/> for further chaining.</returns>
    public static IHealthChecksBuilder AddRemontoireReadiness(
        this IHealthChecksBuilder builder,
        string name = "remontoire-readiness",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null
    ) {
        return builder.Add(new HealthCheckRegistration(name,
            provider => {
                var node = provider.GetRequiredService<RemontoireNode>();
                return new ReadinessCheck(() => node.IsStarted, provider.GetRequiredService<RaftReplicaRegistry>());
            },
            failureStatus, tags ?? ["ready"]));
    }

    /// <summary>
    /// Adds a disk-space check: it fails once the drive of any data directory of the node has less
    /// than <paramref name="minFreeBytes"/> free.
    /// </summary>
    /// <param name="builder">The host's health-checks builder.</param>
    /// <param name="minFreeBytes">The free space below which the check fails.</param>
    /// <param name="name">The check's name.</param>
    /// <param name="failureStatus">The status reported on failure; <see langword="null"/> means <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">The check's tags; <see langword="null"/> means <c>ready</c>.</param>
    /// <returns>The same <paramref name="builder"/> for further chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="minFreeBytes"/> is negative.</exception>
    public static IHealthChecksBuilder AddRemontoireDiskSpace(
        this IHealthChecksBuilder builder,
        long minFreeBytes = DefaultMinFreeDiskSpaceBytes,
        string name = "remontoire-disk-space",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null
    ) {
        ArgumentOutOfRangeException.ThrowIfNegative(minFreeBytes);
        return builder.Add(new HealthCheckRegistration(name,
            provider => {
                var node = provider.GetRequiredService<RemontoireNode>();
                return new DiskSpaceCheck(() => node.DataDirectories, minFreeBytes);
            },
            failureStatus, tags ?? ["ready"]));
    }
}
