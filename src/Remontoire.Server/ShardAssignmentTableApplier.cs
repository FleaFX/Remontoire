using Remontoire.Raft;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// Tails the meta-group's committed log and applies each record to a <see cref="ShardAssignmentTable"/>
/// — a pure forwarder, same shape as <see cref="Messaging.AckIndexApplier"/> one layer down. Lives
/// alongside the hosted service rather than next to the table itself: it needs the replica's own
/// committed-record stream directly (the meta-group has no separate storage-layer log to tail
/// instead), and the table's own project must stay free of that dependency.
/// </summary>
public sealed class ShardAssignmentTableApplier : IAsyncDisposable {
    readonly CancellationTokenSource _cts = new();
    readonly Task _loop;

    /// <summary>
    /// Starts forwarding immediately — every record already committed replays first, same as any
    /// other reader of <see cref="RaftReplica.ReadCommittedAsync"/>. <paramref name="journal"/>,
    /// if given, also receives every record — the committed-record stream allows only one reader,
    /// so a <see cref="MetaLogJournal"/> feeding network subscribers must be fed from here rather
    /// than opening a second, independent read of its own.
    /// </summary>
    public ShardAssignmentTableApplier(RaftReplica metaReplica, ShardAssignmentTable table, MetaLogJournal? journal = null) =>
        _loop = Task.Run(() => RunAsync(metaReplica, table, journal, _cts.Token));

    static async Task RunAsync(RaftReplica metaReplica, ShardAssignmentTable table, MetaLogJournal? journal, CancellationToken cancellationToken) {
        try {
            await foreach (var record in metaReplica.ReadCommittedAsync(cancellationToken)) {
                // Only Append carries an admin command's encoded payload — Raft's own leader-
                // establishing NoOp entries (and any future ShardConfigChange) carry no payload
                // this format can decode, and must never reach it.
                if (record.RecordType != WalRecordType.Append)
                    continue;

                journal?.Append(record.LogicalOffset, record.Payload.ToArray());
                table.Apply(MetaLogRecord.Decode(record.Payload.Span));
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — DisposeAsync cancels and awaits this.
        }
    }

    /// <summary>
    /// Stops the forwarding loop and awaits its shutdown.
    /// </summary>
    public async ValueTask DisposeAsync() {
        await _cts.CancelAsync();
        await _loop;
        _cts.Dispose();
    }
}
