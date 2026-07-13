using FluentAssertions;

namespace Remontoire.Observability;

public class RemontoireMetricsTests {
    [Fact]
    public void Meter_is_named_Remontoire() =>
        RemontoireMetrics.Meter.Name.Should().Be("Remontoire");

    [Fact]
    public void Every_event_driven_instrument_has_its_expected_name_and_unit() {
        RemontoireMetrics.IngestMessagesTotal.Name.Should().Be("remontoire_ingest_messages_total");
        RemontoireMetrics.AckMessagesTotal.Name.Should().Be("remontoire_ack_messages_total");

        RemontoireMetrics.AckLatencySeconds.Name.Should().Be("remontoire_ack_latency_seconds");
        RemontoireMetrics.AckLatencySeconds.Unit.Should().Be("s");

        RemontoireMetrics.WalFsyncDurationSeconds.Name.Should().Be("remontoire_wal_fsync_duration_seconds");
        RemontoireMetrics.WalFsyncDurationSeconds.Unit.Should().Be("s");

        RemontoireMetrics.SegmentCompactionDurationSeconds.Name.Should().Be("remontoire_segment_compaction_duration_seconds");
        RemontoireMetrics.SegmentCompactionDurationSeconds.Unit.Should().Be("s");
    }

    [Fact]
    public void State_snapshot_instrument_name_constants_match_their_metric_names() {
        RemontoireMetrics.QueueDepthName.Should().Be("remontoire_queue_depth");
        RemontoireMetrics.ReplicationLagEntriesName.Should().Be("remontoire_replication_lag_entries");
        RemontoireMetrics.LeaderElectionsTotalName.Should().Be("remontoire_leader_elections_total");
        RemontoireMetrics.RaftTermName.Should().Be("remontoire_raft_term");
        RemontoireMetrics.RaftAppendEntriesSentTotalName.Should().Be("remontoire_raft_append_entries_sent_total");
        RemontoireMetrics.OldestUnackedMessageAgeSecondsName.Should().Be("remontoire_oldest_unacked_message_age_seconds");
        RemontoireMetrics.PruningBlockedByGroupName.Should().Be("remontoire_pruning_blocked_by_group");
        RemontoireMetrics.ForcedPruneMessagesTotalName.Should().Be("remontoire_forced_prune_messages_total");
        RemontoireMetrics.DeadLetterMessagesTotalName.Should().Be("remontoire_dead_letter_messages_total");
    }
}
