using Grpc.Core;
using Remontoire.Engine;
using Remontoire.Raft.V1;
using Remontoire.Storage;

namespace Remontoire.Raft.Grpc;

/// <summary>
/// The server side of the Raft RPCs: resolves each request's <c>group_id</c> via
/// <see cref="RaftReplicaRegistry"/> and dispatches straight to that replica's mailbox, and accepts
/// a meta-log proposal a peer forwards (<c>ProposeMeta</c>). Deliberately
/// thin — no logic of its own beyond that lookup, same as every other transport-facing surface in
/// this codebase (<see cref="RaftReplica"/> itself, not this class, decides everything).
/// </summary>
public sealed class RaftTransportGrpcService(RaftReplicaRegistry registry) : RaftTransport.RaftTransportBase {
    /// <inheritdoc />
    public override async Task<VoteResponse> RequestVote(VoteRequest request, ServerCallContext context) =>
        await Resolve(request.GroupId).ReceiveVoteRequestAsync(request, context.CancellationToken);

    /// <inheritdoc />
    public override async Task<AppendEntriesResponse> AppendEntries(AppendEntriesRequest request, ServerCallContext context) =>
        await Resolve(request.GroupId).ReceiveAppendEntriesAsync(request, context.CancellationToken);

    /// <inheritdoc />
    public override async Task<InstallSnapshotResponse> InstallSnapshot(InstallSnapshotRequest request, ServerCallContext context) =>
        await Resolve(request.GroupId).ReceiveInstallSnapshotAsync(request, context.CancellationToken);

    /// <inheritdoc />
    public override async Task<ProposeMetaResponse> ProposeMeta(ProposeMetaRequest request, ServerCallContext context) {
        if (!registry.TryGet(RemontoireNode.MetaGroupId, out var metaReplica))
            return new ProposeMetaResponse { NotLeader = new ProposeMetaNotLeader() };

        var headers = request.Headers.Select(header => new Header(header.Key.Memory, header.Value.Memory)).ToArray();
        try {
            var result = await metaReplica.ProposeAsync(new AppendRequest(ReadOnlyMemory<byte>.Empty, headers, request.Payload.Memory), context.CancellationToken);
            return new ProposeMetaResponse {
                Success = new ProposeMetaSuccess { RaftIndex = result.RaftIndex, LogicalOffset = result.LogicalOffset, TimestampMicros = result.TimestampMicros },
            };
        } catch (NotLeaderException ex) {
            var notLeader = new ProposeMetaNotLeader();
            if (ex.LeaderHint is { } hint)
                notLeader.LeaderHint = hint;
            return new ProposeMetaResponse { NotLeader = notLeader };
        }
    }

    RaftReplica Resolve(string groupId) => registry.TryGet(groupId, out var replica)
        ? replica
        : throw new RpcException(new Status(StatusCode.NotFound, $"No Raft group '{groupId}' is hosted here."));
}
