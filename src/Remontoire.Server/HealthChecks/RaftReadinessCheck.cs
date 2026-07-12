using Microsoft.Extensions.Diagnostics.HealthChecks;
using Remontoire.Raft.Grpc;

namespace Remontoire.Server.HealthChecks;

/// <summary>
/// Ready as long as at least one hosted group is either this node's leader, or has active contact
/// with its own leader — deliberately an OR across every hosted group, never an AND (see the
/// remarks below), and always evaluated as a single check, never as one registration per group
/// (ASP.NET Core's health-check middleware ANDs every check registered under the same tag, which
/// is the wrong aggregation for the OR this needs across groups).
/// </summary>
/// <remarks>
/// A node hosts many independent physical groups — one stuck group must never pull the whole node
/// out of rotation while every other group is perfectly healthy: Remontoire's own <c>NotLeader</c>/
/// <c>LeaderAddress</c> redirect protocol already isolates a single bad group on its own. A genuine
/// total-partition failure still trips this correctly, since every hosted group fails the same
/// check simultaneously in that case.
/// </remarks>
sealed class RaftReadinessCheck(RaftReplicaRegistry registry) : IHealthCheck {
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        var ready = registry.All.Any(replica => replica.IsLeader || replica.HasActiveLeaderContact);

        return Task.FromResult(ready
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy("No hosted group has an active leader or active leader contact."));
    }
}
