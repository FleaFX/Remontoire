using Microsoft.Extensions.Diagnostics.HealthChecks;
using Remontoire.Raft.Grpc;

namespace Remontoire.Server.HealthChecks;

/// <summary>
/// Fails only when this process is internally wedged — the actor loop of some hosted
/// <c>RaftReplica</c> has stopped processing messages, or a WAL flush has been stuck in-flight
/// unreasonably long. Never fails merely because a group lost its leader/quorum — that's a
/// readiness concern (<see cref="RaftReadinessCheck"/>), not a "restart this process" one.
/// </summary>
sealed class RaftLivenessCheck(RaftReplicaRegistry registry) : IHealthCheck {
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        var now = DateTimeOffset.UtcNow;

        foreach (var replica in registry.All) {
            // A multiple of the group's own election-timeout window — no new number invented,
            // reuses the existing timing parameter as the staleness threshold.
            var threshold = replica.ElectionTimeoutMax * 10;

            if (now - replica.LastActorLoopActivity > threshold)
                return Task.FromResult(HealthCheckResult.Unhealthy($"{replica.GroupId}: actor loop appears stuck."));

            if (replica.WalFlushInProgressSince is { } since && now - since > threshold)
                return Task.FromResult(HealthCheckResult.Unhealthy($"{replica.GroupId}: WAL flush appears stuck."));
        }

        return Task.FromResult(HealthCheckResult.Healthy());
    }
}
