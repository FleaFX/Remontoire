using System.Security.Claims;
using Microsoft.Extensions.Options;
using Remontoire.Sharding;

namespace Remontoire.Security;

/// <summary>
/// Authorization decisions for an authenticated caller — a coarse, cluster-wide role check for
/// the (not yet existing) admin surface, and fine-grained, per-stream produce/consume checks
/// against <see cref="ShardAssignmentTable"/>'s ACL register for the data plane. The two never
/// substitute for each other: <see cref="IsOperator"/> is never consulted by <see cref="CanProduce"/>/
/// <see cref="CanConsume"/> — an operator who needs data-plane access needs an explicit ACL grant
/// like any other subject, or the ACL register would stop meaning anything in a multi-tenant
/// deployment.
/// </summary>
public sealed class RemontoireAuthorizer(ShardAssignmentTable table, IOptions<RemontoireSecurityOptions> options) {
    /// <summary>
    /// Whether <paramref name="user"/> carries <paramref name="role"/> under the configured
    /// <see cref="RemontoireSecurityOptions.RoleClaimType"/>. A generic primitive rather than one
    /// hardcoded to a single role name — <see cref="IsOperator"/> is the only role this project
    /// itself needs, but a caller can check any other role value without any change here.
    /// </summary>
    public bool HasRole(ClaimsPrincipal user, string role) =>
        user.Claims.Any(claim => claim.Type == options.Value.RoleClaimType && claim.Value == role);

    /// <summary>
    /// Whether <paramref name="user"/> carries the configured <see cref="RemontoireSecurityOptions.OperatorRoleValue"/>
    /// role.
    /// </summary>
    public bool IsOperator(ClaimsPrincipal user) => HasRole(user, options.Value.OperatorRoleValue);

    /// <summary>
    /// Whether <paramref name="user"/> may produce onto <paramref name="streamName"/>.
    /// </summary>
    public bool CanProduce(ClaimsPrincipal user, string streamName) =>
        Subject(user, streamName) is { } subject && table.CanProduce(subject, streamName);

    /// <summary>
    /// Whether <paramref name="user"/> may consume/ack as <paramref name="consumerGroup"/> on
    /// <paramref name="streamName"/>.
    /// </summary>
    public bool CanConsume(ClaimsPrincipal user, string streamName, string consumerGroup) =>
        Subject(user, streamName) is { } subject && table.CanConsume(subject, streamName, consumerGroup);

    string? Subject(ClaimsPrincipal user, string streamName) {
        var claimType = table.GetSubjectClaimTypeOverride(streamName) ?? options.Value.SubjectClaimType;
        return user.FindFirst(claimType)?.Value;
    }
}
