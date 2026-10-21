using Grpc.Core;
using Remontoire.Raft.V1;

namespace Remontoire.Raft.Grpc;

/// <summary>
/// The server side of the Raft RPCs: resolves each request's <c>group_id</c> via
/// <see cref="RaftReplicaRegistry"/> and dispatches straight to that replica's mailbox. Deliberately
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

    RaftReplica Resolve(string groupId) => registry.TryGet(groupId, out var replica)
        ? replica
        : throw new RpcException(new Status(StatusCode.NotFound, $"No Raft group '{groupId}' is hosted here."));
}
