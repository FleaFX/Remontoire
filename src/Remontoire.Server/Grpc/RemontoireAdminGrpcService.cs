using System.Security.Cryptography;
using System.Text;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Options;
using Remontoire.Admin.V1;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Security;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server.Grpc;

/// <summary>
/// The administrative, operator-only RPCs (<c>CreateStream</c>, the reshard lifecycle,
/// consumer-group/stream policy, ACL management, and the meta-log audit trail) — every one of
/// them ultimately proposes against the meta-group replica this node hosts (resolved via
/// <see cref="TryGetMetaReplica"/>), the same replica <see cref="ShardAssignmentTableApplier"/>
/// and <see cref="ReshardOrchestrator"/> already use directly. Only ever mapped on a process that
/// also hosts a meta-group replica.
/// </summary>
public sealed class RemontoireAdminGrpcService(
    RaftReplicaRegistry raftRegistry,
    MessagingGroupRegistry messagingRegistry,
    LeaderAddressDirectory leaderAddresses,
    ShardAssignmentTable assignmentTable,
    MetaLogJournal journal,
    ReshardOrchestrator orchestrator,
    IOptions<RemontoireSecurityOptions> securityOptions
) : RemontoireAdmin.RemontoireAdminBase {
    // The meta-group's own, well-known group id — the same literal RaftReplicaHostedService uses
    // everywhere it establishes the meta-group replica.
    const string MetaGroupId = "__meta__";
    internal const string ProposedByHeaderKey = "proposed-by";

    /// <inheritdoc />
    public override async Task<CreateStreamReply> CreateStream(CreateStreamRequest request, ServerCallContext context) {
        if (!TryGetMetaReplica(out var metaReplica))
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "This node does not host a meta-group replica."));

        var routingAlgorithm = MapRoutingAlgorithm(request.RoutingAlgorithm);

        try {
            await metaReplica.ProposeAsync(
                ToAppendRequest(new CreateStream(request.StreamName, request.VirtualShardCount, routingAlgorithm), context), context.CancellationToken);

            foreach (var group in request.InitialGroups) {
                var members = group.Members.Select(member => new ShardGroupMember(member.NodeId, new Uri(member.Address))).ToArray();
                await metaReplica.ProposeAsync(ToAppendRequest(new RegisterGroup(group.GroupId, members), context), context.CancellationToken);
            }

            foreach (var assignment in request.InitialAssignments) {
                var migrationId = DeriveBootstrapMigrationId(request.StreamName, assignment.VirtualShardIndex);
                await metaReplica.ProposeAsync(
                    ToAppendRequest(new MigrationStarted(migrationId, request.StreamName, assignment.VirtualShardIndex, assignment.GroupId, assignment.GroupId), context),
                    context.CancellationToken);
                await metaReplica.ProposeAsync(
                    ToAppendRequest(new Cutover(migrationId, request.StreamName, assignment.VirtualShardIndex, assignment.GroupId), context),
                    context.CancellationToken);
            }

            return new CreateStreamReply { Success = new Empty() };
        } catch (NotLeaderException ex) {
            return new CreateStreamReply { NotLeader = BuildNotLeader(ex) };
        }
    }

    // Deterministic, not Guid.NewGuid(): a retried CreateStream call must reproduce the exact same
    // MigrationId per (stream, virtual shard) so ShardAssignmentTable.Apply's own "same migration
    // already started" / "same migration already cut over" idempotency recognizes a retry as a
    // no-op instead of rejecting it as a conflicting, second migration for the same shard. Not RFC
    // 4122 section 4.3 name-based UUID version 5 — this only needs determinism and collision
    // resistance, not that scheme's specific version/variant bits, so a plain SHA-256-derived Guid
    // is simpler and needs no extra library.
    internal static MigrationId DeriveBootstrapMigrationId(string streamName, int virtualShardIndex) {
        var seed = $"{streamName}:{virtualShardIndex}:bootstrap";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(seed));
        return new MigrationId(new Guid(hash.AsSpan(0, 16)));
    }

    internal static RoutingAlgorithm MapRoutingAlgorithm(RoutingAlgorithmProto algorithm) => algorithm switch {
        RoutingAlgorithmProto.XxHash3V1 => RoutingAlgorithm.XxHash3V1,
        _ => throw new RpcException(new Status(StatusCode.InvalidArgument, $"'{algorithm}' is not a valid routing algorithm.")),
    };

    // Shared by every meta-proposing RPC: attaches the calling subject (the cluster-wide default
    // claim, not the per-stream ACL override RemontoireAuthorizer.Subject resolves) as a
    // "proposed-by" header, reusing AppendRequest.Headers exactly the way
    // RemontoireClientGrpcService.Publish already reuses it for a correlation id.
    AppendRequest ToAppendRequest(MetaLogRecord record, ServerCallContext context) {
        var subject = context.GetHttpContext().User.FindFirst(securityOptions.Value.SubjectClaimType)?.Value;
        IReadOnlyList<Header> headers = subject is null
            ? []
            : [new Header(Encoding.UTF8.GetBytes(ProposedByHeaderKey), Encoding.UTF8.GetBytes(subject))];
        return new AppendRequest(Array.Empty<byte>(), headers, MetaLogRecord.Encode(record));
    }

    bool TryGetMetaReplica(out RaftReplica metaReplica) => raftRegistry.TryGet(MetaGroupId, out metaReplica);

    // Mirrors RemontoireClientGrpcService.BuildNotLeader, generalized from a stream-scoped to a
    // group-scoped NotLeader — every meta-proposing RPC redirects to the meta-group itself, since
    // ex.GroupId is already "__meta__" whenever metaReplica.ProposeAsync is the one that threw.
    NotLeader BuildNotLeader(NotLeaderException ex) =>
        new() { GroupId = ex.GroupId, LeaderAddress = ex.LeaderHint is { } nodeId ? leaderAddresses.TryGet(nodeId)?.ToString() : null };
}
