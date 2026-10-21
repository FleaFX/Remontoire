using Microsoft.Extensions.Diagnostics.HealthChecks;
using Remontoire.Raft.Grpc;

namespace Remontoire.Engine.HealthChecks;

/// <summary>
/// Fails only when this process is internally wedged: the actor loop of a hosted replica has
/// processed nothing, or a WAL flush has been in flight, for longer than <see cref="StuckAfter"/>.
/// Never fails because a group lost its leader or quorum: that is a readiness concern, not a reason
/// to restart the process.
/// </summary>
/// <param name="registry">The replicas this node hosts.</param>
sealed class LivenessCheck(RaftReplicaRegistry registry) : IHealthCheck {
    /// <summary>
    /// How long an actor loop or a WAL flush may stall before this check fails. Fixed rather than a
    /// multiple of a group's election timeout: an embedded group's is a few milliseconds, which a GC
    /// pause or a slow fsync would exceed. An idle leader still processes a heartbeat tick every 50 ms.
    /// </summary>
    internal static readonly TimeSpan StuckAfter = TimeSpan.FromSeconds(5);

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        var now = DateTimeOffset.UtcNow;
        var failures = new List<string>();

        foreach (var replica in registry.All) {
            if (now - replica.LastActorLoopActivity > StuckAfter)
                failures.Add($"{replica.GroupId}: actor loop appears stuck.");

            if (replica.WalFlushInProgressSince is { } since && now - since > StuckAfter)
                failures.Add($"{replica.GroupId}: WAL flush appears stuck.");
        }

        return Task.FromResult(failures.Count == 0
            ? HealthCheckResult.Healthy()
            : new HealthCheckResult(context.Registration.FailureStatus, string.Join("; ", failures)));
    }
}
