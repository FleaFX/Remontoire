using System.Diagnostics.Metrics;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Remontoire.Engine;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;

namespace Remontoire.Cluster;

// Exercised against the real production composition root (RaftReplicaHostedService) rather than
// a hand-rolled test harness: an operator must be able to reconstruct a leader election purely
// from the node's metrics and JSON console log lines — never by reading
// internal state (RaftReplica.LeaderElectionsTotal, etc.) directly. The dead-letter-forward
// scenario gets its own, separate coverage in
// RemontoireGrpcClusterTests (its harness already runs a fast-ticking RetentionEvaluator — this
// one, going through RaftReplicaHostedService's own hardcoded 1-minute default tick, would make
// that scenario needlessly slow here).
[Collection("ConsoleOutput")]
public class ObservabilityEndToEndTests {
    // This test suite runs many test classes in parallel — a literal "group-1" could collide with
    // another test's own same-named group running at the exact same moment. A unique id keeps
    // every measurement assertion below unambiguous regardless of what else is running
    // concurrently.
    const string GroupId = "observability-e2e-group";

    [Fact]
    public async Task A_leader_election_is_reconstructable_purely_via_metrics_and_logs() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);

        var originalOut = Console.Out;
        var logOutput = new StringWriter();
        Console.SetOut(logOutput);
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddJsonConsole(options => options.IncludeScopes = true));

        var raftRegistry = new RaftReplicaRegistry();
        var messagingRegistry = new MessagingGroupRegistry();
        var assignmentTable = new ShardAssignmentTable();
        var metrics = TestNodes.Metrics(raftRegistry, messagingRegistry, assignmentTable);
        var service = new RaftReplicaHostedService(
            Options.Create(new RemontoireClusterOptions {
                Groups = [new RaftGroupOptions { GroupId = GroupId, NodeId = "node-1", DataDirectory = directory }],
            }),
            TestNodes.Node(raftRegistry, messagingRegistry, assignmentTable, metrics: metrics, loggerFactory: loggerFactory),
            new LeaderAddressDirectory(), assignmentTable, loggerFactory);

        try {
            await service.StartAsync(CancellationToken.None);
            try {
                // A single-node group elects itself via its own real election timer (250-500ms,
                // RaftReplicaHostedService's own hardcoded config) — nothing here forces it.
                raftRegistry.TryGet(GroupId, out var replica).Should().BeTrue();
                (await RunUntilAsync(() => replica.IsLeader, TimeSpan.FromSeconds(5))).Should().BeTrue();

                var measurements = new List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)>();
                using var listener = new MeterListener {
                    InstrumentPublished = (instrument, meterListener) => {
                        if (ReferenceEquals(instrument.Meter, metrics.Meter))
                            meterListener.EnableMeasurementEvents(instrument);
                    },
                };
                listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray())));
                listener.Start();
                listener.RecordObservableInstruments();

                measurements.Where(m => m.Name == "remontoire.raft.leader_elections" && m.Tags.Contains(new KeyValuePair<string, object?>("group", GroupId)))
                    .Should().ContainSingle(m => m.Value == 1, "purely from the metric — no direct read of RaftReplica.LeaderElectionsTotal");
                logOutput.ToString().Should().Contain($"Became leader of {GroupId}",
                    "purely from the JSON log line — no direct read of internal replica state");
                logOutput.ToString().Should().Contain($"\"GroupId\":\"{GroupId}\"",
                    "the per-group BeginScope must attach GroupId to this very log line");
            } finally {
                await service.StopAsync(CancellationToken.None);
            }
        } finally {
            Console.SetOut(originalOut);
            Directory.Delete(directory, recursive: true);
        }
    }

    static Task<bool> RunUntilAsync(Func<bool> condition, TimeSpan timeout) =>
        ConditionPoller.WaitUntilAsync(condition, timeout, TimeSpan.FromMilliseconds(20));
}
