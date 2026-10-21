using Microsoft.Extensions.Diagnostics.HealthChecks;
using Remontoire.Raft.Grpc;

namespace Remontoire.Engine.HealthChecks;

/// <summary>
/// Ready once the node has started (an embedded node: once every group caught up), until it starts
/// stopping, as long as at least one hosted replica leads its group or has active contact with its
/// leader. An OR across the groups, never an AND, in one check rather than one per group (the
/// health-check middleware ANDs every check under a tag): one stuck group must not take a node out
/// of rotation while its other groups are healthy — Remontoire's redirect protocol already isolates
/// a single bad group. A total partition still fails it, since every group fails at once.
/// </summary>
/// <param name="isStarted">Whether the node has started and not begun stopping.</param>
/// <param name="registry">The replicas this node hosts.</param>
sealed class ReadinessCheck(Func<bool> isStarted, RaftReplicaRegistry registry) : IHealthCheck {
    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        if (!isStarted())
            return Task.FromResult(new HealthCheckResult(context.Registration.FailureStatus, "The node has not started, or is stopping."));

        return Task.FromResult(registry.All.Any(replica => replica.IsLeader || replica.HasActiveLeaderContact)
            ? HealthCheckResult.Healthy()
            : new HealthCheckResult(context.Registration.FailureStatus, "No hosted group has an active leader or active leader contact."));
    }
}
