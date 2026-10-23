using System.Diagnostics.Metrics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Raft.V1;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

// The metrics reference: the exact set of instruments a host can scrape, and the values the state
// callbacks observe. Every listener filters by meter reference, never by name — each test's
// RemontoireMetrics owns its own IMeterFactory-created meter, so parallel test classes stay
// invisible to each other.
public class RemontoireMetricsTests {
    [Fact]
    public void Every_instrument_has_its_documented_name_unit_and_type() {
        var metrics = TestNodes.Metrics(new RaftReplicaRegistry(), new MessagingGroupRegistry(), new ShardAssignmentTable());

        metrics.Meter.Name.Should().Be(RemontoireMetrics.MeterName);

        var instruments = new List<Instrument>();
        using var listener = new MeterListener {
            InstrumentPublished = (instrument, meterListener) => {
                if (!ReferenceEquals(instrument.Meter, metrics.Meter))
                    return;

                meterListener.EnableMeasurementEvents(instrument);
                instruments.Add(instrument);
            },
        };
        listener.Start();

        var expected = new (string Name, string? Unit, Type Type)[] {
            ("remontoire.messages.published", "{message}", typeof(Counter<long>)),
            ("remontoire.messages.acked", "{message}", typeof(Counter<long>)),
            ("remontoire.messages.retried", "{message}", typeof(Counter<long>)),
            ("remontoire.messages.parked", "{message}", typeof(Counter<long>)),
            ("remontoire.ack.latency", "s", typeof(Histogram<double>)),
            ("remontoire.wal.fsync.duration", "s", typeof(Histogram<double>)),
            ("remontoire.segment.compaction.duration", "s", typeof(Histogram<double>)),
            ("remontoire.raft.term", null, typeof(ObservableGauge<long>)),
            ("remontoire.raft.leader", null, typeof(ObservableGauge<long>)),
            ("remontoire.raft.leader_elections", "{election}", typeof(ObservableCounter<long>)),
            ("remontoire.raft.replication.lag", "{entry}", typeof(ObservableGauge<long>)),
            ("remontoire.raft.append_entries.sent", "{request}", typeof(ObservableCounter<long>)),
            ("remontoire.consumer_group.lag", "{message}", typeof(ObservableGauge<long>)),
            ("remontoire.consumer_group.oldest_unacked_age", "s", typeof(ObservableGauge<double>)),
            ("remontoire.consumer_group.blocking_pruning", null, typeof(ObservableGauge<long>)),
            ("remontoire.messages.force_pruned", "{message}", typeof(ObservableCounter<long>)),
            ("remontoire.messages.dead_lettered", "{message}", typeof(ObservableCounter<long>)),
        };

        instruments.Should().HaveCount(expected.Length);
        foreach (var (name, unit, type) in expected) {
            var instrument = instruments.Should().ContainSingle(i => i.Name == name, $"the {name} instrument must exist exactly once").Which;
            instrument.Unit.Should().Be(unit);
            instrument.GetType().Should().Be(type);
        }
    }

    [Fact]
    public async Task Observes_every_state_metric_from_the_real_registries() {
        var raftRegistry = new RaftReplicaRegistry();
        var messagingRegistry = new MessagingGroupRegistry();
        var assignmentTable = new ShardAssignmentTable(); // no assignments registered — the stream tag falls back to the group id

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
                        // consumer_group.lag/oldest_unacked_age/blocking_pruning.
                        await replica.ProposeAsync(new AckRequest("fast-group", [first.LogicalOffset, second.LogicalOffset]));
                        (await WaitUntilAsync(() => ackIndex.GetOrCreate("fast-group").CommittedWatermark == 2)).Should().BeTrue();
                        ackIndex.GetOrCreate("slow-group"); // registers it, watermark stays 0

                        var metrics = TestNodes.Metrics(raftRegistry, messagingRegistry, assignmentTable);

                        var measurements = new List<(string Name, object Value, KeyValuePair<string, object?>[] Tags)>();
                        using var listener = new MeterListener {
                            InstrumentPublished = (instrument, meterListener) => {
                                if (ReferenceEquals(instrument.Meter, metrics.Meter))
                                    meterListener.EnableMeasurementEvents(instrument);
                            },
                        };
                        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
                        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
                        listener.Start();
                        listener.RecordObservableInstruments();

                        measurements.Where(m => m.Name == "remontoire.raft.term").Should().ContainSingle(m =>
                            (long)m.Value == 1 && m.Tags.Contains(new KeyValuePair<string, object?>("group", "shard-a")));

                        measurements.Where(m => m.Name == "remontoire.raft.leader").Should().ContainSingle(m =>
                            (long)m.Value == 1 && m.Tags.Contains(new KeyValuePair<string, object?>("group", "shard-a")));

                        measurements.Where(m => m.Name == "remontoire.raft.leader_elections" && m.Tags.Contains(new KeyValuePair<string, object?>("group", "shard-a")))
                            .Should().ContainSingle(m => (long)m.Value == 1);

                        // At least one send (the term-opening NoOp) plus one per real propose below —
                        // exact count isn't asserted (a heartbeat/retry could add more); only that
                        // the metric is wired and counts something real for the real peer.
                        measurements.Where(m => m.Name == "remontoire.raft.append_entries.sent").Should().ContainSingle(m =>
                            (long)m.Value >= 1 && m.Tags.Contains(new KeyValuePair<string, object?>("peer", "node-2")));

                        measurements.Should().NotContain(m => m.Name == "remontoire.raft.replication.lag", "shard-a is the leader — meaningless for it");

                        measurements.Where(m => m.Name == "remontoire.consumer_group.lag" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "fast-group")))
                            .Should().ContainSingle(m => (long)m.Value == 0);
                        measurements.Where(m => m.Name == "remontoire.consumer_group.lag" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "slow-group")))
                            .Should().ContainSingle(m => (long)m.Value == 2);

                        measurements.Where(m => m.Name == "remontoire.consumer_group.blocking_pruning" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "slow-group")))
                            .Should().ContainSingle(m => (long)m.Value == 1, "slow-group has the lowest committed watermark — it's the one blocking pruning");
                        measurements.Where(m => m.Name == "remontoire.consumer_group.blocking_pruning" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "fast-group")))
                            .Should().ContainSingle(m => (long)m.Value == 0);

                        measurements.Where(m => m.Name == "remontoire.consumer_group.oldest_unacked_age" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "slow-group")))
                            .Should().ContainSingle(m => (double)m.Value >= 0, "offset 0 is still unacked and really exists");
                        measurements.Where(m => m.Name == "remontoire.consumer_group.oldest_unacked_age" && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "fast-group")))
                            .Should().ContainSingle(m => (double)m.Value == 0, "fast-group's watermark caught up to NextOffsetToApply — nothing unacked to age");

                        measurements.Where(m => m.Name == "remontoire.messages.force_pruned" && m.Tags.Contains(new KeyValuePair<string, object?>("group", "shard-a")))
                            .Should().ContainSingle(m => (long)m.Value == 0);
                        measurements.Where(m => m.Name == "remontoire.messages.dead_lettered" && m.Tags.Contains(new KeyValuePair<string, object?>("group", "shard-a")))
                            .Should().ContainSingle(m => (long)m.Value == 0);

                        // Once slow-group caught up too, the slowest group holds nothing back.
                        await replica.ProposeAsync(new AckRequest("slow-group", [first.LogicalOffset, second.LogicalOffset]));
                        (await WaitUntilAsync(() => ackIndex.GetOrCreate("slow-group").CommittedWatermark == 2)).Should().BeTrue();
                        measurements.Clear();
                        listener.RecordObservableInstruments();

                        measurements.Where(m => m.Name == "remontoire.consumer_group.blocking_pruning")
                            .Should().HaveCount(2).And.OnlyContain(m => (long)m.Value == 0, "every group acked everything");
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

    [Fact]
    public async Task Two_instances_observe_only_their_own_registries() {
        var firstRegistry = new RaftReplicaRegistry();
        var secondRegistry = new RaftReplicaRegistry();
        await using var firstReplica = await StartFollowerAsync(firstRegistry, "group-a");
        await using var secondReplica = await StartFollowerAsync(secondRegistry, "group-b");

        var first = TestNodes.Metrics(firstRegistry, new MessagingGroupRegistry(), new ShardAssignmentTable());
        var second = TestNodes.Metrics(secondRegistry, new MessagingGroupRegistry(), new ShardAssignmentTable());

        var firstMeasurements = new List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)>();
        var secondMeasurements = new List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)>();
        using var firstListener = Listen(first, firstMeasurements);
        using var secondListener = Listen(second, secondMeasurements);
        firstListener.RecordObservableInstruments();
        secondListener.RecordObservableInstruments();

        firstMeasurements.Where(m => m.Name == "remontoire.raft.term").Should().ContainSingle(m =>
            m.Tags.Contains(new KeyValuePair<string, object?>("group", "group-a")));
        secondMeasurements.Where(m => m.Name == "remontoire.raft.term").Should().ContainSingle(m =>
            m.Tags.Contains(new KeyValuePair<string, object?>("group", "group-b")));
        firstMeasurements.Should().NotContain(m => m.Tags.Contains(new KeyValuePair<string, object?>("group", "group-b")));
        secondMeasurements.Should().NotContain(m => m.Tags.Contains(new KeyValuePair<string, object?>("group", "group-a")));
    }

    [Fact]
    public async Task Consumer_group_metrics_are_observed_only_where_the_replica_leads() {
        var raftRegistry = new RaftReplicaRegistry();
        var messagingRegistry = new MessagingGroupRegistry();
        var assignmentTable = new ShardAssignmentTable();

        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        try {
            // A replica that never receives an ElectionTimeoutElapsed never becomes leader.
            var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(),
                new RaftReplicaConfig(GroupId: "group-1", NodeId: "node-1", Peers: [],
                    HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11)));
            await replica.StartAsync();
            try {
                raftRegistry.Register(replica);

                var ackIndex = new AckIndex();
                ackIndex.GetOrCreate("g"); // registers a consumer group the metrics could observe
                var shardLog = await ShardLog.OpenAsync(directory, replica.ReadCommittedAsync,
                    compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())));
                try {
                    await using var applier = new AckIndexApplier(shardLog, ackIndex);
                    var retentionEvaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                        ShardLog: shardLog, AckIndex: ackIndex, IsMandatory: _ => true, GetMaxRetention: () => TimeSpan.MaxValue,
                        ForwardToDeadLetterAsync: (_, _) => Task.FromResult(false), IsAdmissionPaused: () => false, IsLeader: () => replica.IsLeader));
                    try {
                        messagingRegistry.Register("group-1", shardLog, ackIndex, retentionEvaluator);

                        var metrics = TestNodes.Metrics(raftRegistry, messagingRegistry, assignmentTable);
                        var measurements = new List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)>();
                        using var listener = Listen(metrics, measurements);
                        listener.RecordObservableInstruments();

                        measurements.Should().NotContain(m => m.Name == "remontoire.consumer_group.lag",
                            "the replica does not lead — a group's consumer groups are observed where its leader lives");
                        measurements.Should().NotContain(m => m.Name == "remontoire.consumer_group.oldest_unacked_age");
                        measurements.Should().NotContain(m => m.Name == "remontoire.consumer_group.blocking_pruning");
                        measurements.Where(m => m.Name == "remontoire.raft.leader").Should().ContainSingle(m =>
                            m.Value == 0 && m.Tags.Contains(new KeyValuePair<string, object?>("group", "group-1")));
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

    [Fact]
    public async Task An_embedded_host_records_publish_and_ack_on_its_own_meter() {
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        IHost? host = null;
        try {
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddRemontoire().AddEmbedded(options => options.DataDirectory = dataDirectory);
            host = builder.Build();

            var metrics = host.Services.GetRequiredService<RemontoireMetrics>();
            var measurements = new List<(string Name, object Value, KeyValuePair<string, object?>[] Tags)>();
            using var listener = new MeterListener {
                InstrumentPublished = (instrument, meterListener) => {
                    if (ReferenceEquals(instrument.Meter, metrics.Meter))
                        meterListener.EnableMeasurementEvents(instrument);
                },
            };
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
            listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
            listener.Start();

            await host.StartAsync();
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("metrics");
            await admin.EnsureConsumerGroupAsync("metrics", "metrics-group");

            await producer.PublishAsync("metrics", "key-1", "one"u8.ToArray());
            var message = await FirstAsync(consumer, "metrics", "metrics-group");
            await consumer.AckAsync("metrics", "metrics-group", message.ShardId, message.Offset);

            measurements.Where(m => m.Name == "remontoire.messages.published").Should().ContainSingle(m =>
                (long)m.Value == 1
                && m.Tags.Contains(new KeyValuePair<string, object?>("stream", "metrics"))
                && m.Tags.Contains(new KeyValuePair<string, object?>("group", "metrics")));
            measurements.Where(m => m.Name == "remontoire.messages.acked").Should().ContainSingle(m =>
                (long)m.Value == 1 && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "metrics-group")));
            measurements.Where(m => m.Name == "remontoire.ack.latency").Should().ContainSingle(m => (double)m.Value >= 0);
            measurements.Where(m => m.Name == "remontoire.wal.fsync.duration").Should().Contain(m =>
                m.Tags.Contains(new KeyValuePair<string, object?>("group", "__meta__")));
        } finally {
            if (host is not null) {
                await host.StopAsync();
                host.Dispose();
            }
            Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task An_embedded_host_records_retried_and_parked_on_its_own_meter() {
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        IHost? host = null;
        try {
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddRemontoire().AddEmbedded(options => options.DataDirectory = dataDirectory);
            host = builder.Build();

            var metrics = host.Services.GetRequiredService<RemontoireMetrics>();
            var measurements = new List<(string Name, object Value, KeyValuePair<string, object?>[] Tags)>();
            using var listener = new MeterListener {
                InstrumentPublished = (instrument, meterListener) => {
                    if (ReferenceEquals(instrument.Meter, metrics.Meter))
                        meterListener.EnableMeasurementEvents(instrument);
                },
            };
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
            listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
            listener.Start();

            await host.StartAsync();
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("metrics");
            await admin.EnsureConsumerGroupAsync("metrics", "metrics-group",
                new ConsumerGroupOptions { MaxRetries = 1, InitialRetryDelay = TimeSpan.FromMilliseconds(10) });

            await producer.PublishAsync("metrics", "key-1", "one"u8.ToArray());

            await using var messages = new ConsumeStream(consumer.ConsumeAsync("metrics", "metrics-group"));
            var first = await NextAsync(messages);
            first.Attempt.Should().Be(1);
            await consumer.NackAsync("metrics", "metrics-group", first.ShardId, first.Offset); // within MaxRetries: offered again
            var second = await NextAsync(messages);
            second.Attempt.Should().Be(2);
            await consumer.NackAsync("metrics", "metrics-group", second.ShardId, second.Offset, reason: "boom"); // past MaxRetries: parked

            measurements.Where(m => m.Name == "remontoire.messages.retried").Should().ContainSingle(m =>
                (long)m.Value == 1
                && m.Tags.Contains(new KeyValuePair<string, object?>("stream", "metrics"))
                && m.Tags.Contains(new KeyValuePair<string, object?>("group", "metrics"))
                && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "metrics-group")));
            measurements.Where(m => m.Name == "remontoire.messages.parked").Should().ContainSingle(m =>
                (long)m.Value == 1
                && m.Tags.Contains(new KeyValuePair<string, object?>("stream", "metrics"))
                && m.Tags.Contains(new KeyValuePair<string, object?>("group", "metrics"))
                && m.Tags.Contains(new KeyValuePair<string, object?>("consumer_group", "metrics-group")));
        } finally {
            if (host is not null) {
                await host.StopAsync();
                host.Dispose();
            }
            Directory.Delete(dataDirectory, recursive: true);
        }
    }

    static Task<RemontoireMessage> NextAsync(ConsumeStream messages) => messages.NextAsync();

    static async Task<RaftReplica> StartFollowerAsync(RaftReplicaRegistry registry, string groupId) {
        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(),
            new RaftReplicaConfig(GroupId: groupId, NodeId: "node-1", Peers: [],
                HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11)));
        await replica.StartAsync();
        registry.Register(replica);
        return replica;
    }

    static MeterListener Listen(RemontoireMetrics metrics, List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> measurements) {
        var listener = new MeterListener {
            InstrumentPublished = (instrument, meterListener) => {
                if (ReferenceEquals(instrument.Meter, metrics.Meter))
                    meterListener.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
        listener.Start();
        return listener;
    }

    static async Task<RemontoireMessage> FirstAsync(IRemontoireConsumer consumer, string streamName, string consumerGroup) {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var message in consumer.ConsumeAsync(streamName, consumerGroup, cts.Token))
            return message;

        throw new InvalidOperationException($"No message arrived on stream '{streamName}' for group '{consumerGroup}'.");
    }

    static Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null) =>
        ConditionPoller.WaitUntilAsync(condition, timeout ?? TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(5));
}
