using System.Diagnostics.Metrics;

namespace Remontoire.Observability;

/// <summary>
/// The single <see cref="Meter"/> instance and the full set of instrument definitions for this
/// product. Recording code lives elsewhere (<c>Remontoire.Server</c>, the only layer with DI
/// visibility onto the registries/tables the values come from) — this type only ever declares
/// instruments, never observes state or wires a callback itself.
/// </summary>
public static class RemontoireMetrics {
    public static readonly Meter Meter = new("Remontoire", "1.0.0");

    // Event-driven — recorded inline at the call site that produces the event.
    public static readonly Counter<long> IngestMessagesTotal =
        Meter.CreateCounter<long>("remontoire_ingest_messages_total");
    public static readonly Counter<long> AckMessagesTotal =
        Meter.CreateCounter<long>("remontoire_ack_messages_total");
    public static readonly Histogram<double> AckLatencySeconds =
        Meter.CreateHistogram<double>("remontoire_ack_latency_seconds", unit: "s");
    public static readonly Histogram<double> WalFsyncDurationSeconds =
        Meter.CreateHistogram<double>("remontoire_wal_fsync_duration_seconds", unit: "s");
    public static readonly Histogram<double> SegmentCompactionDurationSeconds =
        Meter.CreateHistogram<double>("remontoire_segment_compaction_duration_seconds", unit: "s");

    // State-snapshot — ObservableGauge/ObservableCounter registration itself happens once in
    // Program.cs (the callbacks need DI-resolved registries); this type only names them.
    // remontoire_raft_append_entries_sent_total belongs here, not above: it's read off
    // RaftReplica.AppendEntriesSentTotal (a per-peer dictionary already maintained on the actor),
    // scraped via a callback over RaftReplicaRegistry.All — never recorded inline.
    public const string QueueDepthName = "remontoire_queue_depth";
    public const string ReplicationLagEntriesName = "remontoire_replication_lag_entries";
    public const string LeaderElectionsTotalName = "remontoire_leader_elections_total";
    public const string RaftTermName = "remontoire_raft_term";
    public const string RaftAppendEntriesSentTotalName = "remontoire_raft_append_entries_sent_total";
    public const string OldestUnackedMessageAgeSecondsName = "remontoire_oldest_unacked_message_age_seconds";
    public const string PruningBlockedByGroupName = "remontoire_pruning_blocked_by_group";
    public const string ForcedPruneMessagesTotalName = "remontoire_forced_prune_messages_total";
    public const string DeadLetterMessagesTotalName = "remontoire_dead_letter_messages_total";
}
