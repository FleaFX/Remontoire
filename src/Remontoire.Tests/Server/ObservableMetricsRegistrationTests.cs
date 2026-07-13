using System.Diagnostics.Metrics;
using FluentAssertions;
using Remontoire.Messaging;
using Remontoire.Observability;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Raft.V1;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

// Layer 3: real RaftReplica/ShardLog/AckIndex/RetentionEvaluator instances, registered into real
// registries, then Register()'d — the same composition RaftReplicaHostedService builds in
// production, collapsed into one process, no gRPC involved. Callbacks are pulled directly via
// MeterListener.RecordObservableInstruments(), never through a real scrape.
public class ObservableMetricsRegistrationTests {
    [Fact]
    public async Task Register_wires_every_state_snapshot_metric_from_the_real_registries() {
        var raftRegistry = new RaftReplicaRegistry();
        var messagingRegistry = new MessagingGroupRegistry();
        var assignmentTable = new ShardAssignmentTable(); // no assignments registered — ResolveStreamName always null

        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        try {
            // node-2 auto-acknowledges every AppendEntries — every subsequent propose (the NoOp,
            // then the two real records) commits without needing a hand-injected response per entry.
            var transport = new RecordingRaftTransport { OnAppendEntries = (_, request) => new AppendEntriesResponse { Term = request.Term, Success = true } };
            var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), transport,
                new RaftReplicaConfig(GroupId: "shard-a", NodeId: "node-1", Peers: [new RaftGroupMember("node-2", new Uri("https://node-2.local"))],
                    HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11)));
            await replica.StartAsync();
            try {
                replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration));
                await replica.DrainAsync();
                replica.TryPost(new VoteResponseReceived("node-2", new VoteResponse { Term = 1, VoteGranted = true }, 1));
                await replica.DrainAsync(); // -> leader (self + node-2 satisfy quorum), sends the term-opening NoOp to node-2
                (await WaitUntilAsync(() => replica.IsLeader)).Should().BeTrue("node-2's auto-ack of the term-opening NoOp must commit it");
                raftRegistry.Register(replica);

                var ackIndex = new AckIndex();
                var shardLog = await ShardLog.OpenAsync(directory, replica.ReadCommittedAsync,
                    compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())));
                try {
                    await using var applier = new AckIndexApplier(shardLog, ackIndex);
                    var retentionEvaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                        ShardLog: shardLog, AckIndex: ackIndex, IsMandatory: _ => true, GetMaxRetention: () => TimeSpan.MaxValue,
                        ForwardToDeadLetterAsync: (_, _) => Task.FromResult(false), IsAdmissionPaused: () => false, IsLeader: () => replica.IsLeader));
                    try {
                        messagingRegistry.Register("shard-a", shardLog, ackIndex, retentionEvaluator);

                        var first = await replica.ProposeAsync(new AppendRequest(ReadOnlyMemory<byte>.Empty, [], "one"u8.ToArray()));
                        var second = await replica.ProposeAsync(new AppendRequest(ReadOnlyMemory<byte>.Empty, [], "two"u8.ToArray()));
                        (await WaitUntilAsync(() => shardLog.NextOffsetToApply > second.LogicalOffset)).Should().BeTrue();

                        // fast-group acks everything; slow-group never acks — the two ends of
                        // queue_depth/oldest_unacked_message_age_seconds/pruning_blocked_by_group.
                        await replica.ProposeAsync(new AckRequest("fast-group", [first.LogicalOffset, second.LogicalOffset]));
                        (await WaitUntilAsync(() => ackIndex.GetOrCreate("fast-group").CommittedWatermark == 2)).Should().BeTrue();
                        ackIndex.GetOrCreate("slow-group"); // registers it, watermark stays 0

                        ObservableMetricsRegistration.Register(raftRegistry, messagingRegistry, assignmentTable);

                        var measurements = new List<(string Name, object Value, KeyValuePair<string, object?>[] Tags)>();
                        using var listener = new MeterListener {
                            InstrumentPublished = (instrument, meterListener) => {
                                if (instrument.Meter.Name == RemontoireMetrics.Meter.Name)
                                    meterListener.EnableMeasurementEvents(instrument);
                            },
                        };
                        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
                        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
                        listener.Start();
                        listener.RecordObservableInstruments();

                        measurements.Where(m => m.Name == "remontoire_raft_term").Should().ContainSingle(m =>
                            (long)m.Value == 1 && m.Tags.Contains(new KeyValuePair<string, object?>("shard", "shard-a")));

                        measurements.Where(m => m.Name == "remontoire_leader_elections_total" && m.Tags.Contains(new KeyValuePair<string, object?>("shard", "shard-a")))
                            .Should().ContainSingle(m => (long)m.Value == 1);

                        // At least one send (the term-opening NoOp) plus one per real propose below —
                        // exact count isn't asserted (a heartbeat/retry could add more); only that
                        // the metric is wired and counts something real for the real peer.
                        measurements.Where(m => m.Name == "remontoire_raft_append_entries_sent_total").Should().ContainSingle(m =>
                            (long)m.Value >= 1 && m.Tags.Contains(new KeyValuePair<string, object?>("peer_node_id", "node-2")));

                        measurements.Should().NotContain(m => m.Name == "remontoire_replication_lag_entries", "shard-a is the leader — meaningless for it");

                        measurements.Where(m => m.Name == "remontoire_queue_depth" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "fast-group")))
                            .Should().ContainSingle(m => (long)m.Value == 0);
                        measurements.Where(m => m.Name == "remontoire_queue_depth" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "slow-group")))
                            .Should().ContainSingle(m => (long)m.Value == 2);

                        measurements.Where(m => m.Name == "remontoire_pruning_blocked_by_group" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "slow-group")))
                            .Should().ContainSingle(m => (long)m.Value == 1, "slow-group has the lowest committed watermark — it's the one blocking pruning");
                        measurements.Where(m => m.Name == "remontoire_pruning_blocked_by_group" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "fast-group")))
                            .Should().ContainSingle(m => (long)m.Value == 0);

                        measurements.Where(m => m.Name == "remontoire_oldest_unacked_message_age_seconds" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "slow-group")))
                            .Should().ContainSingle(m => (double)m.Value >= 0, "offset 0 is still unacked and really exists");
                        measurements.Where(m => m.Name == "remontoire_oldest_unacked_message_age_seconds" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "fast-group")))
                            .Should().ContainSingle(m => (double)m.Value == 0, "fast-group's watermark caught up to NextOffsetToApply — nothing unacked to age");

                        measurements.Where(m => m.Name == "remontoire_forced_prune_messages_total" && m.Tags.Contains(new KeyValuePair<string, object?>("shard", "shard-a")))
                            .Should().ContainSingle(m => (long)m.Value == 0);
                        measurements.Where(m => m.Name == "remontoire_dead_letter_messages_total" && m.Tags.Contains(new KeyValuePair<string, object?>("shard", "shard-a")))
                            .Should().ContainSingle(m => (long)m.Value == 0);
                    } finally {
                        await retentionEvaluator.DisposeAsync();
                    }
                } finally {
                    await shardLog.DisposeAsync();
                }
            } finally {
                await replica.DisposeAsync();
            }
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null) {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(2));
        while (DateTime.UtcNow < deadline) {
            if (condition())
                return true;
            await Task.Delay(5);
        }
        return condition();
    }
}
