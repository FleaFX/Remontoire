using System.Diagnostics.Metrics;
using Remontoire.Messaging;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

/// <summary>
/// The metrics of one Remontoire node: one <see cref="System.Diagnostics.Metrics.Meter"/> named
/// <see cref="MeterName"/>, created by the host's <see cref="IMeterFactory"/>, so every service
/// provider has its own. Nothing is measured until a listener (the host's OpenTelemetry setup, for
/// example) enables an instrument; the state metrics are callbacks, evaluated only when collected.
/// </summary>
public sealed class RemontoireMetrics {
    /// <summary>
    /// The meter's name, for <c>AddMeter</c>.
    /// </summary>
    public const string MeterName = "Remontoire";

    readonly Counter<long> _messagesPublished;
    readonly Counter<long> _messagesAcked;
    readonly Counter<long> _messagesRetried;
    readonly Counter<long> _messagesParked;
    readonly Histogram<double> _ackLatency;
    readonly Histogram<double> _walFsyncDuration;
    readonly Histogram<double> _segmentCompactionDuration;

    /// <summary>
    /// Creates the meter and every instrument, and registers the state callbacks over the registries.
    /// </summary>
    /// <param name="meterFactory">Creates the meter; it lives as long as the factory does.</param>
    /// <param name="raftRegistry">The replicas the Raft metrics read.</param>
    /// <param name="messagingRegistry">The groups the consumer-group and retention metrics read.</param>
    /// <param name="assignmentTable">Resolves a group's stream and a consumer group's policy.</param>
    public RemontoireMetrics(
        IMeterFactory meterFactory,
        RaftReplicaRegistry raftRegistry,
        MessagingGroupRegistry messagingRegistry,
        ShardAssignmentTable assignmentTable
    ) {
        Meter = meterFactory.Create(MeterName);
        _messagesPublished = Meter.CreateCounter<long>("remontoire.messages.published", "{message}", "Messages committed by a publish.");
        _messagesAcked = Meter.CreateCounter<long>("remontoire.messages.acked", "{message}", "Offsets acknowledged by a consumer group.");
        _messagesRetried = Meter.CreateCounter<long>("remontoire.messages.retried", "{message}", "Nacked messages that will be offered to their consumer group again.");
        _messagesParked = Meter.CreateCounter<long>("remontoire.messages.parked", "{message}", "Messages a consumer group parked after their last retry.");
        _ackLatency = Meter.CreateHistogram<double>("remontoire.ack.latency", "s", "Time from a message's ingest to its ack.");
        _walFsyncDuration = Meter.CreateHistogram<double>("remontoire.wal.fsync.duration", "s", "Duration of one WAL fsync.");
        _segmentCompactionDuration = Meter.CreateHistogram<double>("remontoire.segment.compaction.duration", "s", "Duration of one segment compaction.");

        ObserveReplicas(raftRegistry);
        ObserveGroups(raftRegistry, messagingRegistry, assignmentTable);
    }

    /// <summary>
    /// The meter every instrument of this node belongs to.
    /// </summary>
    internal Meter Meter { get; }

    /// <summary>
    /// Records one committed publish.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="groupId">The group that committed it.</param>
    internal void RecordPublished(string streamName, string groupId) =>
        _messagesPublished.Add(1, Tag("stream", streamName), Tag("group", groupId));

    /// <summary>
    /// Records acknowledged offsets.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="groupId">The group serving the stream.</param>
    /// <param name="consumerGroup">The consumer group that acknowledged.</param>
    /// <param name="count">The number of acknowledged offsets.</param>
    internal void RecordAcked(string streamName, string groupId, string consumerGroup, int count) =>
        _messagesAcked.Add(count, Tag("stream", streamName), Tag("group", groupId), Tag("consumer_group", consumerGroup));

    /// <summary>
    /// Records one nacked message that will be offered again.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="groupId">The group serving the stream.</param>
    /// <param name="consumerGroup">The consumer group that nacked.</param>
    internal void RecordRetried(string streamName, string groupId, string consumerGroup) =>
        _messagesRetried.Add(1, Tag("stream", streamName), Tag("group", groupId), Tag("consumer_group", consumerGroup));

    /// <summary>
    /// Records one parked message.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="groupId">The group serving the stream.</param>
    /// <param name="consumerGroup">The consumer group that parked it.</param>
    internal void RecordParked(string streamName, string groupId, string consumerGroup) =>
        _messagesParked.Add(1, Tag("stream", streamName), Tag("group", groupId), Tag("consumer_group", consumerGroup));

    /// <summary>
    /// Records the time from one message's ingest to its ack.
    /// </summary>
    /// <param name="streamName">The stream.</param>
    /// <param name="groupId">The group serving the stream.</param>
    /// <param name="seconds">The latency in seconds.</param>
    internal void RecordAckLatency(string streamName, string groupId, double seconds) =>
        _ackLatency.Record(seconds, Tag("stream", streamName), Tag("group", groupId));

    /// <summary>
    /// Records one WAL fsync.
    /// </summary>
    /// <param name="groupId">The group whose WAL was synced.</param>
    /// <param name="duration">How long the fsync took.</param>
    internal void RecordWalFsync(string groupId, TimeSpan duration) =>
        _walFsyncDuration.Record(duration.TotalSeconds, Tag("group", groupId));

    /// <summary>
    /// Records one segment compaction.
    /// </summary>
    /// <param name="groupId">The group whose segments were compacted.</param>
    /// <param name="duration">How long the compaction took.</param>
    internal void RecordSegmentCompaction(string groupId, TimeSpan duration) =>
        _segmentCompactionDuration.Record(duration.TotalSeconds, Tag("group", groupId));

    void ObserveReplicas(RaftReplicaRegistry raftRegistry) {
        Meter.CreateObservableGauge("remontoire.raft.term",
            () => raftRegistry.All.Select(replica => new Measurement<long>((long)replica.CurrentTerm, Tag("group", replica.GroupId))),
            description: "The replica's current Raft term.");

        Meter.CreateObservableGauge("remontoire.raft.leader",
            () => raftRegistry.All.Select(replica => new Measurement<long>(replica.IsLeader ? 1 : 0, Tag("group", replica.GroupId))),
            description: "1 while the replica leads its group, 0 otherwise.");

        Meter.CreateObservableCounter("remontoire.raft.leader_elections",
            () => raftRegistry.All.Select(replica => new Measurement<long>(replica.LeaderElectionsTotal, Tag("group", replica.GroupId))),
            "{election}", "Times the replica became leader.");

        // A leader knows its own commit index: only followers lag, and a leader's row would always
        // read zero.
        Meter.CreateObservableGauge("remontoire.raft.replication.lag",
            () => raftRegistry.All.Where(replica => !replica.IsLeader).Select(replica => new Measurement<long>(
                Math.Max(0L, (long)replica.LeaderKnownCommitIndex - (long)replica.CommitIndex), Tag("group", replica.GroupId))),
            "{entry}", "Entries the leader committed that this follower has not committed yet.");

        // Heartbeats and real replication share one send path (RaftReplica.Leader.cs), so this
        // counts both.
        Meter.CreateObservableCounter("remontoire.raft.append_entries.sent",
            () => raftRegistry.All.SelectMany(replica => replica.AppendEntriesSentTotal.Select(sent => new Measurement<long>(sent.Value,
                Tag("group", replica.GroupId), Tag("peer", sent.Key)))),
            "{request}", "AppendEntries requests sent to each peer, heartbeats included.");
    }

    void ObserveGroups(RaftReplicaRegistry raftRegistry, MessagingGroupRegistry messagingRegistry, ShardAssignmentTable assignmentTable) {
        string? StreamName(string groupId) =>
            RemontoireNode.PickPrimaryStreamName(assignmentTable.EnumerateAssignments().Where(assignment => assignment.GroupId == groupId));

        // Every replica of a group holds the same watermarks: observed only where the replica leads,
        // so a cluster reports each consumer group once, from the node whose retention acts on it.
        IEnumerable<(string GroupId, string? StreamName, ShardLog ShardLog, AckIndex AckIndex)> LedGroups() =>
            messagingRegistry.All
                .Where(group => raftRegistry.TryGet(group.GroupId, out var replica) && replica.IsLeader)
                .Select(group => (group.GroupId, StreamName(group.GroupId), group.ShardLog, group.AckIndex));

        // CommittedWatermark, not LowWatermark: what Raft committed, never a possibly isolated
        // leader's optimistic local state. In checkpoint mode it trails the acks by up to one checkpoint.
        Meter.CreateObservableGauge("remontoire.consumer_group.lag",
            () => LedGroups().SelectMany(group => group.AckIndex.RegisteredConsumerGroups().Select(consumerGroup => new Measurement<long>(
                (long)(group.ShardLog.NextOffsetToApply - group.AckIndex.GetOrCreate(consumerGroup).CommittedWatermark),
                Tag("stream", group.StreamName ?? group.GroupId), Tag("group", group.GroupId), Tag("consumer_group", consumerGroup)))),
            "{message}", "Messages the consumer group has not acked yet.");

        Meter.CreateObservableGauge("remontoire.consumer_group.oldest_unacked_age",
            () => LedGroups().SelectMany(group => group.AckIndex.RegisteredConsumerGroups().Select(consumerGroup => new Measurement<double>(
                OldestUnackedAgeSeconds(group.ShardLog, group.AckIndex.GetOrCreate(consumerGroup).CommittedWatermark),
                Tag("stream", group.StreamName ?? group.GroupId), Tag("group", group.GroupId), Tag("consumer_group", consumerGroup)))),
            "s", "Age of the oldest message the consumer group has not acked yet; 0 when there is none.");

        // The one mandatory group AckIndex.SlowestMandatoryGroup names gets 1, but only while it
        // still has unacked messages: a slowest group that caught up holds nothing back. Every other
        // registered group gets 0; a best-effort group never blocks pruning, so it always reads 0.
        Meter.CreateObservableGauge("remontoire.consumer_group.blocking_pruning",
            () => LedGroups().SelectMany(group => {
                bool IsMandatory(string consumerGroup) =>
                    group.StreamName is null || assignmentTable.GetConsumerGroupPolicy(group.StreamName, consumerGroup).Mandatory;
                var blocking = group.AckIndex.SlowestMandatoryGroup(IsMandatory) is { } slowest
                    && slowest.CommittedWatermark < group.ShardLog.NextOffsetToApply
                        ? slowest.ConsumerGroup
                        : null;

                return group.AckIndex.RegisteredConsumerGroups().Select(consumerGroup => new Measurement<long>(
                    consumerGroup == blocking ? 1 : 0,
                    Tag("stream", group.StreamName ?? group.GroupId), Tag("group", group.GroupId), Tag("consumer_group", consumerGroup)));
            }),
            description: "1 for the slowest mandatory consumer group while it has unacked messages and so holds back pruning, 0 otherwise.");

        Meter.CreateObservableCounter("remontoire.messages.force_pruned",
            () => messagingRegistry.All.Select(group => new Measurement<long>(group.ShardLog.ForcedPruneMessagesTotal, Tag("group", group.GroupId))),
            "{message}", "Messages pruned by the emergency size limit before every mandatory group acked them.");

        // One reason for now: the retention forward. A parked message is not dead-lettered; it has
        // its own counter, remontoire.messages.parked.
        Meter.CreateObservableCounter("remontoire.messages.dead_lettered",
            () => messagingRegistry.All.Select(group => new Measurement<long>(group.RetentionEvaluator.DeadLetterMessagesTotal,
                Tag("stream", StreamName(group.GroupId) ?? group.GroupId), Tag("group", group.GroupId), Tag("reason", "max_retention"))),
            "{message}", "Messages forwarded to the dead-letter stream after their maximum retention.");
    }

    static double OldestUnackedAgeSeconds(ShardLog shardLog, ulong committedWatermark) {
        if (!shardLog.TryGet(committedWatermark, out var handle))
            return 0; // nothing unacked (the watermark caught up to NextOffsetToApply): no age to report

        using (handle) {
            var nowMicros = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
            // Both operands cast to long before subtracting: clock skew between the ingesting leader
            // and this node can make the timestamp exceed now, which ulong would wrap.
            return Math.Max(0L, (long)nowMicros - (long)handle.Entry.TimestampMicros) / 1_000_000.0;
        }
    }

    static KeyValuePair<string, object?> Tag(string key, object? value) => new(key, value);
}
