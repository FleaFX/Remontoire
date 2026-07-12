using System.Diagnostics.Metrics;
using Remontoire.Observability;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;

namespace Remontoire.Server;

/// <summary>
/// Registers every state-snapshot metric as an <c>ObservableGauge</c>/<c>ObservableCounter</c>
/// callback on <see cref="RemontoireMetrics.Meter"/> — each evaluated only when something actually
/// scrapes <c>/metrics</c>, never on its own polling loop. Lives in <c>Remontoire.Server</c>, not
/// <c>Remontoire.Observability</c>, because it needs DI-resolved registries/tables that
/// <c>Remontoire.Observability</c> deliberately never references.
/// </summary>
static class ObservableMetricsRegistration {
    /// <summary>
    /// Registers every instrument once. Safe to call exactly once per process — a second call
    /// against the same <see cref="RemontoireMetrics.Meter"/> would register duplicate callbacks,
    /// each contributing its own (identical) measurement per scrape.
    /// </summary>
    public static void Register(RaftReplicaRegistry raftRegistry, MessagingGroupRegistry messagingRegistry, ShardAssignmentTable assignmentTable) {
        string? ResolveStreamName(string groupId) =>
            RaftReplicaHostedService.PickPrimaryStreamName(assignmentTable.EnumerateAssignments().Where(a => a.GroupId == groupId));

        RemontoireMetrics.Meter.CreateObservableGauge(RemontoireMetrics.RaftTermName, () =>
            raftRegistry.All.Select(replica => new Measurement<long>((long)replica.CurrentTerm,
                new KeyValuePair<string, object?>("shard", replica.GroupId))));

        RemontoireMetrics.Meter.CreateObservableCounter(RemontoireMetrics.LeaderElectionsTotalName, () =>
            raftRegistry.All.Select(replica => new Measurement<long>(replica.LeaderElectionsTotal,
                new KeyValuePair<string, object?>("shard", replica.GroupId))));

        // Meaningless on a leader itself (it already knows its own commit index) — only followers
        // are measured; a leader's own row would always read zero and add nothing but noise.
        RemontoireMetrics.Meter.CreateObservableGauge(RemontoireMetrics.ReplicationLagEntriesName, () =>
            raftRegistry.All.Where(replica => !replica.IsLeader).Select(replica => new Measurement<long>(
                Math.Max(0L, (long)replica.LeaderKnownCommitIndex - (long)replica.CommitIndex),
                new KeyValuePair<string, object?>("shard", replica.GroupId))));

        // Heartbeats and real replication share the one SendAppendEntriesAsync call site
        // (RaftReplica.Leader.cs), so this counts both identically — no separate heartbeat RPC
        // exists to count instead.
        RemontoireMetrics.Meter.CreateObservableCounter(RemontoireMetrics.RaftAppendEntriesSentTotalName, () =>
            raftRegistry.All.SelectMany(replica => replica.AppendEntriesSentTotal.Select(sent => new Measurement<long>(sent.Value,
                new KeyValuePair<string, object?>("shard", replica.GroupId),
                new KeyValuePair<string, object?>("peer_node_id", sent.Key)))));

        RemontoireMetrics.Meter.CreateObservableGauge(RemontoireMetrics.QueueDepthName, () =>
            messagingRegistry.All.SelectMany(group => {
                var streamName = ResolveStreamName(group.GroupId) ?? group.GroupId;
                return group.AckIndex.RegisteredConsumerGroups().Select(consumerGroup => new Measurement<long>(
                    (long)(group.ShardLog.NextOffsetToApply - group.AckIndex.GetOrCreate(consumerGroup).CommittedWatermark),
                    new KeyValuePair<string, object?>("stream", streamName),
                    new KeyValuePair<string, object?>("shard", group.GroupId),
                    new KeyValuePair<string, object?>("consumer_group", consumerGroup)));
            }));

        // CommittedWatermark, not LowWatermark — this feeds the pruning-blocked alert, so it must
        // reflect what's actually Raft-committed, never an isolated, possibly-stale leader's own
        // optimistic local state.
        RemontoireMetrics.Meter.CreateObservableGauge(RemontoireMetrics.OldestUnackedMessageAgeSecondsName, () =>
            messagingRegistry.All.SelectMany(group => {
                var streamName = ResolveStreamName(group.GroupId) ?? group.GroupId;
                return group.AckIndex.RegisteredConsumerGroups().Select(consumerGroup => new Measurement<double>(
                    OldestUnackedAgeSeconds(group.ShardLog, group.AckIndex.GetOrCreate(consumerGroup).CommittedWatermark),
                    new KeyValuePair<string, object?>("stream", streamName),
                    new KeyValuePair<string, object?>("shard", group.GroupId),
                    new KeyValuePair<string, object?>("consumer_group", consumerGroup)));
            }));

        // The one mandatory group AckIndex.SlowestMandatoryGroup names gets 1; every other
        // registered group (mandatory or best-effort) gets 0 — a best-effort group can never
        // block pruning by construction, so it always reads 0 here.
        RemontoireMetrics.Meter.CreateObservableGauge(RemontoireMetrics.PruningBlockedByGroupName, () =>
            messagingRegistry.All.SelectMany(group => {
                var streamName = ResolveStreamName(group.GroupId);
                bool IsMandatory(string consumerGroup) => streamName is null || assignmentTable.GetConsumerGroupPolicy(streamName, consumerGroup).Mandatory;
                var slowest = group.AckIndex.SlowestMandatoryGroup(IsMandatory);

                return group.AckIndex.RegisteredConsumerGroups().Select(consumerGroup => new Measurement<long>(
                    consumerGroup == slowest?.ConsumerGroup ? 1 : 0,
                    new KeyValuePair<string, object?>("stream", streamName ?? group.GroupId),
                    new KeyValuePair<string, object?>("shard", group.GroupId),
                    new KeyValuePair<string, object?>("consumer_group", consumerGroup)));
            }));

        RemontoireMetrics.Meter.CreateObservableCounter(RemontoireMetrics.ForcedPruneMessagesTotalName, () =>
            messagingRegistry.All.Select(group => new Measurement<long>(group.ShardLog.ForcedPruneMessagesTotal,
                new KeyValuePair<string, object?>("shard", group.GroupId))));

        RemontoireMetrics.Meter.CreateObservableCounter(RemontoireMetrics.DeadLetterMessagesTotalName, () =>
            messagingRegistry.All.Select(group => new Measurement<long>(group.RetentionEvaluator.DeadLetterMessagesTotal,
                new KeyValuePair<string, object?>("shard", group.GroupId))));
    }

    static double OldestUnackedAgeSeconds(Storage.ShardLog shardLog, ulong committedWatermark) {
        if (!shardLog.TryGet(committedWatermark, out var handle))
            return 0; // nothing unacked (watermark caught up to NextOffsetToApply) — no age to report

        using (handle) {
            var nowMicros = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
            return (nowMicros - handle.Entry.TimestampMicros) / 1_000_000.0;
        }
    }
}
