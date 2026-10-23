using System.Text;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Remontoire.Admin.V1;
using Remontoire.Engine;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// The administrative RPCs (declaring streams and consumer groups, the reshard lifecycle, and the
/// meta-log audit trail). The declarations run through <see cref="NodeAdmin"/>, which forwards to
/// the meta-group leader itself; the reshard RPCs propose against the meta-group replica this node
/// hosts and answer with a redirect when it doesn't lead. Only ever mapped on a process that
/// also hosts a meta-group replica. Who may call them is the host's decision, on the mapped
/// endpoint (for example <c>RequireAuthorization</c>).
/// </summary>
public sealed class RemontoireAdminGrpcService(
    NodeAdmin admin,
    RaftReplicaRegistry raftRegistry,
    MessagingGroupRegistry messagingRegistry,
    LeaderAddressDirectory leaderAddresses,
    ShardAssignmentTable assignmentTable,
    MetaLogJournal journal,
    ReshardOrchestrator orchestrator
) : RemontoireAdmin.RemontoireAdminBase {
    // The meta-group's own, well-known group id — the same literal RaftReplicaHostedService uses
    // everywhere it establishes the meta-group replica.
    const string MetaGroupId = "__meta__";

    /// <inheritdoc />
    public override async Task<Empty> EnsureStream(EnsureStreamRequest request, ServerCallContext context) {
        if (request.MaxRetention is null)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "max_retention is required."));

        var options = new StreamOptions {
            MaxRetention = request.MaxRetention.ToTimeSpan(),
            MaxSizeBytesPerVirtualShard = request.HasMaxSizeBytesPerVirtualShard ? request.MaxSizeBytesPerVirtualShard : null,
            CheckpointInterval = request.CheckpointInterval?.ToTimeSpan(),
            CheckpointOffsetCount = request.HasCheckpointOffsetCount ? request.CheckpointOffsetCount : null,
        };
        await RunAsync(() => admin.EnsureStreamAsync(request.StreamName, options, ProposedBy(context), context.CancellationToken));
        return new Empty();
    }

    /// <inheritdoc />
    public override async Task<Empty> EnsureConsumerGroup(EnsureConsumerGroupRequest request, ServerCallContext context) {
        if (request.InitialRetryDelay is null || request.MaxRetryDelay is null)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "initial_retry_delay and max_retry_delay are required."));

        var options = new ConsumerGroupOptions {
            AckMode = MapAckMode(request.AckMode), Mandatory = request.Mandatory, MaxRetries = request.MaxRetries,
            InitialRetryDelay = request.InitialRetryDelay.ToTimeSpan(), MaxRetryDelay = request.MaxRetryDelay.ToTimeSpan(),
        };
        await RunAsync(() => admin.EnsureConsumerGroupAsync(request.StreamName, request.ConsumerGroup, options, ProposedBy(context), context.CancellationToken));
        return new Empty();
    }

    /// <inheritdoc />
    public override async Task<StartReshardReply> StartReshard(StartReshardRequest request, ServerCallContext context) {
        if (!TryGetMetaReplica(out var metaReplica))
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "This node does not host a meta-group replica."));

        var migrationId = ParseMigrationId(request.MigrationId);
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

        var migrationId = ParseMigrationId(request.MigrationId);
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

        var migrationId = ParseMigrationId(request.MigrationId);
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

    // request.MigrationId is accepted on the wire (for the caller's own correlation across the
    // reshard step sequence) but deliberately not read here — CopyRecordsAsync only needs
    // from_group_id/to_group_id/from_offset, and only one migration is ever expected to be moving
    // data between a given FROM/TO pair at a time.
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

        var migrationId = ParseMigrationId(request.MigrationId);
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

    /// <inheritdoc />
    public override async Task ListMetaLogRecords(ListMetaLogRecordsRequest request, IServerStreamWriter<MetaLogRecordView> responseStream, ServerCallContext context) {
        await foreach (var (version, payload, timestampMicros, headers) in journal.WatchAsync(request.FromVersion, context.CancellationToken)) {
            var record = MetaLogRecord.Decode(payload);
            var (recordType, fields) = Describe(record);

            var view = new MetaLogRecordView {
                Version = version, RecordType = recordType, TimestampMicros = timestampMicros,
                ProposedBy = FindProposedBy(headers) ?? "",
            };
            view.Fields.Add(fields);
            await responseStream.WriteAsync(view);
        }
    }

    // Mirrors MetaLogRecord.WriteBody's own switch shape, read-only — one case per MetaLogRecord,
    // grows the same way that switch does whenever a new record type is added.
    internal static (string RecordType, Dictionary<string, string> Fields) Describe(MetaLogRecord record) => record switch {
        CreateStream r => (nameof(CreateStream), new Dictionary<string, string> {
            ["StreamName"] = r.StreamName, ["VirtualShardCount"] = r.VirtualShardCount.ToString(), ["RoutingAlgorithm"] = r.RoutingAlgorithm.ToString(),
        }),
        RegisterGroup r => (nameof(RegisterGroup), new Dictionary<string, string> {
            ["GroupId"] = r.GroupId, ["Members"] = string.Join(", ", r.Members.Select(member => $"{member.NodeId}@{member.Address}")),
        }),
        MigrationStarted r => (nameof(MigrationStarted), new Dictionary<string, string> {
            ["MigrationId"] = r.MigrationId.Value.ToString(), ["StreamName"] = r.StreamName, ["VirtualShardIndex"] = r.VirtualShardIndex.ToString(),
            ["FromGroupId"] = r.FromGroupId, ["ToGroupId"] = r.ToGroupId,
        }),
        MigrationAborted r => (nameof(MigrationAborted), new Dictionary<string, string> {
            ["MigrationId"] = r.MigrationId.Value.ToString(), ["StreamName"] = r.StreamName, ["VirtualShardIndex"] = r.VirtualShardIndex.ToString(),
        }),
        Cutover r => (nameof(Cutover), new Dictionary<string, string> {
            ["MigrationId"] = r.MigrationId.Value.ToString(), ["StreamName"] = r.StreamName, ["VirtualShardIndex"] = r.VirtualShardIndex.ToString(), ["ToGroupId"] = r.ToGroupId,
        }),
        MigrationCompleted r => (nameof(MigrationCompleted), new Dictionary<string, string> {
            ["MigrationId"] = r.MigrationId.Value.ToString(), ["StreamName"] = r.StreamName, ["VirtualShardIndex"] = r.VirtualShardIndex.ToString(),
        }),
        SetConsumerGroupAckMode r => (nameof(SetConsumerGroupAckMode), new Dictionary<string, string> {
            ["StreamName"] = r.StreamName, ["ConsumerGroup"] = r.ConsumerGroup, ["Mode"] = r.Mode.ToString(),
        }),
        SetConsumerGroupMandatory r => (nameof(SetConsumerGroupMandatory), new Dictionary<string, string> {
            ["StreamName"] = r.StreamName, ["ConsumerGroup"] = r.ConsumerGroup, ["Mandatory"] = r.Mandatory.ToString(),
        }),
        SetStreamRetentionPolicy r => (nameof(SetStreamRetentionPolicy), new Dictionary<string, string> {
            ["StreamName"] = r.StreamName, ["AuditRetention"] = r.AuditRetention.ToString(), ["MaxRetention"] = r.MaxRetention.ToString(),
            ["MaxSizeBytesPerVirtualShard"] = r.MaxSizeBytesPerVirtualShard?.ToString() ?? "",
        }),
        SetStreamCheckpointInterval r => (nameof(SetStreamCheckpointInterval), new Dictionary<string, string> {
            ["StreamName"] = r.StreamName, ["Interval"] = r.Interval?.ToString() ?? "", ["OffsetCount"] = r.OffsetCount?.ToString() ?? "",
        }),
        DeclareConsumerGroup r => (nameof(DeclareConsumerGroup), new Dictionary<string, string> {
            ["StreamName"] = r.StreamName, ["ConsumerGroup"] = r.ConsumerGroup, ["Mode"] = r.Mode.ToString(), ["Mandatory"] = r.Mandatory.ToString(),
            ["MaxRetries"] = r.MaxRetries.ToString(), ["InitialRetryDelay"] = r.InitialRetryDelay.ToString(), ["MaxRetryDelay"] = r.MaxRetryDelay.ToString(),
        }),
        _ => throw new ArgumentOutOfRangeException(nameof(record), record, "Unknown MetaLogRecord case."),
    };

    // A small, deliberate duplicate of RemontoireClientGrpcService.HeaderKeyEquals's own
    // byte-comparison idiom rather than an extracted shared helper — that method is private to its
    // own class, and one extra call site here doesn't justify restructuring either class just to
    // share four lines.
    internal static string? FindProposedBy(IReadOnlyList<Header> headers) {
        foreach (var header in headers) {
            if (Encoding.UTF8.GetByteCount(RemontoireNode.ProposedByHeaderKey) != header.Key.Length || Encoding.UTF8.GetString(header.Key.Span) != RemontoireNode.ProposedByHeaderKey)
                continue;
            return Encoding.UTF8.GetString(header.Value.Span);
        }
        return null;
    }

    /// <summary>
    /// The status a declaration's failure answers with; <see langword="null"/> for an exception
    /// that is no declaration failure, which then surfaces as it would anyway.
    /// </summary>
    /// <param name="exception">The failure.</param>
    /// <returns>The status, or <see langword="null"/>.</returns>
    internal static Status? ToStatus(Exception exception) => exception switch {
        ArgumentException => new Status(StatusCode.InvalidArgument, exception.Message),
        RemontoireConflictException => new Status(StatusCode.AlreadyExists, exception.Message),
        RemontoireUnavailableException or TimeoutException => new Status(StatusCode.Unavailable, exception.Message),
        InvalidOperationException => new Status(StatusCode.FailedPrecondition, exception.Message),
        _ => null,
    };

    static async Task RunAsync(Func<Task> declaration) {
        try {
            await declaration();
        } catch (Exception ex) when (ToStatus(ex) is { } status) {
            throw new RpcException(status);
        }
    }

    // The caller's name as the host's authentication resolved it (User.Identity.Name, so the host
    // picks the claim through its NameClaimType); an anonymous call names nobody.
    static string? ProposedBy(ServerCallContext context) => context.GetHttpContext().User.Identity?.Name;

    internal static AckMode MapAckMode(AckModeProto mode) => mode switch {
        AckModeProto.Strict => AckMode.Strict,
        AckModeProto.Checkpoint => AckMode.Checkpoint,
        _ => throw new RpcException(new Status(StatusCode.InvalidArgument, $"'{mode}' is not a valid ack mode.")),
    };

    internal static MigrationId ParseMigrationId(string value) =>
        Guid.TryParse(value, out var guid) ? new MigrationId(guid) : throw new RpcException(new Status(StatusCode.InvalidArgument, $"'{value}' is not a valid migration_id."));

    bool TryGetMetaReplica(out RaftReplica metaReplica) => raftRegistry.TryGet(MetaGroupId, out metaReplica);

    // Mirrors RemontoireClientGrpcService.BuildNotLeader, generalized from a stream-scoped to a
    // group-scoped NotLeader — every meta-proposing RPC redirects to the meta-group itself, since
    // ex.GroupId is already "__meta__" whenever metaReplica.ProposeAsync is the one that threw.
    //
    // Never assign LeaderAddress = null directly: an "optional string" field's generated setter
    // still rejects null via ProtoPreconditions.CheckNotNull (only the separate ClearLeaderAddress
    // accepts a truly absent value) — leaving the property untouched is the correct way to keep it
    // unset when no address is known.
    NotLeader BuildNotLeader(NotLeaderException ex) {
        var notLeader = new NotLeader { GroupId = ex.GroupId };
        if (ex.LeaderHint is { } nodeId && leaderAddresses.TryGet(nodeId) is { } address)
            notLeader.LeaderAddress = address.ToString();
        return notLeader;
    }

    // Mirrors RemontoireClientGrpcService.BuildGroupRedirect — used where no NotLeaderException was
    // ever thrown (the local precondition failed before any ProposeAsync call was even attempted,
    // e.g. Cutover's own FROM-group-not-hosted-here guard). Same null-avoidance as BuildNotLeader.
    NotLeader BuildGroupRedirect(string groupId) {
        var notLeader = new NotLeader { GroupId = groupId };
        if (assignmentTable.TryGetGroup(groupId, out var group))
            foreach (var member in group.Members) {
                if (leaderAddresses.TryGet(member.NodeId) is not { } address)
                    continue;
                notLeader.LeaderAddress = address.ToString();
                break;
            }

        return notLeader;
    }
}
