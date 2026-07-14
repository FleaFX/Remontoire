using Google.Protobuf;
using Grpc.Core;
using Remontoire.Meta.V1;

namespace Remontoire.Server.Grpc;

/// <summary>
/// The over-the-wire read path for the meta-group's assignment table — <c>Watch</c> for the
/// primary live tail, <c>GetSnapshot</c> for initial fill and periodic reconciliation. Thin: both
/// RPCs read straight from a <see cref="MetaLogJournal"/>, never touch a <see cref="Raft.RaftReplica"/>
/// directly. Only ever mapped on a process that also hosts a meta-group replica.
/// </summary>
public sealed class ShardAssignmentMetaGrpcService(MetaLogJournal journal) : ShardAssignmentMeta.ShardAssignmentMetaBase {
    /// <inheritdoc />
    public override async Task Watch(WatchRequest request, IServerStreamWriter<MetaLogRecordProto> responseStream, ServerCallContext context) {
        await foreach (var (version, payload, _, _) in journal.WatchAsync(request.FromVersion, context.CancellationToken))
            await responseStream.WriteAsync(ToProto(version, payload));
    }

    /// <inheritdoc />
    public override Task<ShardAssignmentSnapshot> GetSnapshot(GetSnapshotRequest request, ServerCallContext context) {
        var (version, records) = journal.Snapshot();

        var snapshot = new ShardAssignmentSnapshot { Version = version };
        snapshot.Records.Add(records.Select(record => ToProto(record.Version, record.Payload)));
        return Task.FromResult(snapshot);
    }

    static MetaLogRecordProto ToProto(ulong version, byte[] payload) =>
        new() { Version = version, Payload = ByteString.CopyFrom(payload) };
}
