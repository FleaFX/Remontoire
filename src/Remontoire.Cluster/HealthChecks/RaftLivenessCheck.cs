using Microsoft.Extensions.Diagnostics.HealthChecks;
using Remontoire.Raft.Grpc;

namespace Remontoire.Cluster.HealthChecks;

/// <summary>
/// Fails only when this process is internally wedged — the actor loop of some hosted
/// <c>RaftReplica</c> has stopped processing messages, or a WAL flush has been stuck in-flight
/// unreasonably long. Never fails merely because a group lost its leader/quorum — that's a
/// readiness concern (<see cref="RaftReadinessCheck"/>), not a "restart this process" one.
/// </summary>
sealed class RaftLivenessCheck(RaftReplicaRegistry registry) : IHealthCheck {
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        var now = DateTimeOffset.UtcNow;
        var failures = new List<string>();

        foreach (var replica in registry.All) {
            // A multiple of the group's own election-timeout window — no new number invented,
            // reuses the existing timing parameter as the staleness threshold.
            var threshold = replica.ElectionTimeoutMax * 10;

            if (now - replica.LastActorLoopActivity > threshold)
                failures.Add($"{replica.GroupId}: actor loop appears stuck.");

            if (replica.WalFlushInProgressSince is { } since && now - since > threshold)
                failures.Add($"{replica.GroupId}: WAL flush appears stuck.");
        }

        return Task.FromResult(failures.Count == 0
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy(string.Join("; ", failures)));
    }
}
