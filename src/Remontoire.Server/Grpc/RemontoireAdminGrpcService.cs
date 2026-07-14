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

    /// <inheritdoc />
    public override async Task<StartReshardReply> StartReshard(StartReshardRequest request, ServerCallContext context) {
        if (!TryGetMetaReplica(out var metaReplica))
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "This node does not host a meta-group replica."));

        var migrationId = new MigrationId(Guid.Parse(request.MigrationId));
        try {
            await orchestrator.ProposeMigrationStartedAsync(
                metaReplica, migrationId, request.StreamName, request.VirtualShardIndex, request.FromGroupId, request.ToGroupId, context.CancellationToken);
            return new StartReshardReply { Success = new Empty() };
        } catch (NotLeaderException ex) {
            return new StartReshardReply { NotLeader = BuildNotLeader(ex) };
        }
    }

    /// <inheritdoc />
    public override async Task<CompleteReshardReply> CompleteReshard(CompleteReshardRequest request, ServerCallContext context) {
        if (!TryGetMetaReplica(out var metaReplica))
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "This node does not host a meta-group replica."));

        var migrationId = new MigrationId(Guid.Parse(request.MigrationId));
        try {
            await orchestrator.ProposeMigrationCompletedAsync(metaReplica, migrationId, request.StreamName, request.VirtualShardIndex, context.CancellationToken);
            return new CompleteReshardReply { Success = new Empty() };
        } catch (NotLeaderException ex) {
            return new CompleteReshardReply { NotLeader = BuildNotLeader(ex) };
        }
    }

    /// <inheritdoc />
    public override async Task<AbortReshardReply> AbortReshard(AbortReshardRequest request, ServerCallContext context) {
        if (!TryGetMetaReplica(out var metaReplica))
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "This node does not host a meta-group replica."));

        var migrationId = new MigrationId(Guid.Parse(request.MigrationId));
        try {
            await orchestrator.ProposeMigrationAbortedAsync(metaReplica, migrationId, request.StreamName, request.VirtualShardIndex, context.CancellationToken);
            return new AbortReshardReply { Success = new Empty() };
        } catch (NotLeaderException ex) {
            return new AbortReshardReply { NotLeader = BuildNotLeader(ex) };
        }
    }

    // No "proposed-by" header on these three, nor on CopyReshardData/Cutover below:
    // ReshardOrchestrator's own methods build their AppendRequest internally (always with an empty
    // header list) — their signature stays unchanged (see this file's own remarks on ReshardOrchestrator
    // above), so ListMetaLogRecords shows an empty proposed_by for every migration-lifecycle record,
    // permanently, not just until some later step fills it in.

    /// <inheritdoc />
    public override async Task CopyReshardData(CopyReshardDataRequest request, IServerStreamWriter<CopyReshardDataProgress> responseStream, ServerCallContext context) {
        var fromOffset = request.FromOffset;

        while (true) {
            ulong nextOffset;
            try {
                nextOffset = await orchestrator.CopyRecordsAsync(request.FromGroupId, request.ToGroupId, fromOffset, context.CancellationToken);
            } catch (NotLeaderException ex) {
                // The gap CopyRecordsAsync's own doc-comment names: destination.ProposeAsync throws
                // this uncaught today — this is where it finally gets translated, redirecting to the
                // TO group specifically (not the meta-group), matching where the underlying
                // ProposeAsync call actually happened.
                await responseStream.WriteAsync(new CopyReshardDataProgress { NotLeader = BuildNotLeader(ex) });
                return;
            } catch (InvalidOperationException ex) {
                // Neither TryGet call inside CopyRecordsAsync succeeded (or one of the two didn't) —
                // a co-location precondition distinct from a leadership redirect: there is no group
                // to redirect to, this node simply doesn't host the data this call needs.
                throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
            }

            var caughtUp = nextOffset == fromOffset;
            await responseStream.WriteAsync(new CopyReshardDataProgress { Progress = new ProgressUpdate { NextOffset = nextOffset, CaughtUp = caughtUp } });
            if (caughtUp)
                return;

            fromOffset = nextOffset;
        }
    }

    /// <inheritdoc />
    public override async Task<CutoverReply> Cutover(CutoverRequest request, ServerCallContext context) {
        if (!TryGetMetaReplica(out var metaReplica))
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "This node does not host a meta-group replica."));

        // MigrationAdmissionGate is a purely local, per-process gate — pausing on a node that
        // doesn't host the FROM group's messaging state would pause nothing real. Same precondition
        // CopyRecordsAsync already enforces via messagingRegistry.TryGet.
        if (!messagingRegistry.TryGet(request.FromGroupId, out _))
            return new CutoverReply { NotLeader = BuildGroupRedirect(request.FromGroupId) };

        var migrationId = new MigrationId(Guid.Parse(request.MigrationId));
        var timeout = request.Timeout.ToTimeSpan();

        using var pauseScope = orchestrator.PauseAdmission(request.FromGroupId);
        try {
            await orchestrator.ProposeCutoverAsync(metaReplica, migrationId, request.StreamName, request.VirtualShardIndex, request.ToGroupId, context.CancellationToken);
        } catch (NotLeaderException ex) {
            return new CutoverReply { NotLeader = BuildNotLeader(ex) };
        }

        // ConditionPoller's first production consumer — waits for this node's own, locally
        // materialized ShardAssignmentTable to observe the routing flip this call just proposed.
        var observed = await ConditionPoller.WaitUntilAsync(
            () => assignmentTable.TryGetAssignment(request.StreamName, request.VirtualShardIndex, out var assignment) && assignment.GroupId == request.ToGroupId,
            timeout, cancellationToken: context.CancellationToken);

        // pauseScope disposes on every exit path from here, whether the cutover was observed in
        // time or not — PauseAdmission's own remarks are explicit that resuming any later than "as
        // soon as this group's own routing reflects the flip" reopens the exact write-loss window
        // the pause exists to close, so timing out must resume anyway, never leave the group paused
        // indefinitely.
        return observed ? new CutoverReply { Success = new Empty() } : new CutoverReply { TimedOut = new CutoverTimedOut() };
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

    // Mirrors RemontoireClientGrpcService.BuildGroupRedirect — used where no NotLeaderException was
    // ever thrown (the local precondition failed before any ProposeAsync call was even attempted,
    // e.g. Cutover's own FROM-group-not-hosted-here guard).
    NotLeader BuildGroupRedirect(string groupId) {
        Uri? address = null;
        if (assignmentTable.TryGetGroup(groupId, out var group))
            foreach (var member in group.Members) {
                address = leaderAddresses.TryGet(member.NodeId);
                if (address is not null)
                    break;
            }

        return new NotLeader { GroupId = groupId, LeaderAddress = address?.ToString() };
    }
}
