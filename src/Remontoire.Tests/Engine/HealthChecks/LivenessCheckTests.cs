using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Time.Testing;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;

namespace Remontoire.Engine.HealthChecks;

public class LivenessCheckTests {
    [Fact]
    public async Task Healthy_when_no_replica_is_registered() {
        var check = new LivenessCheck(new RaftReplicaRegistry());

        var result = await check.CheckHealthAsync(Context(check));

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Healthy_when_the_actor_loop_started_recently() {
        var registry = new RaftReplicaRegistry();
        await using var replica = await StartAsync("node-1"); // real TimeProvider.System — just stamped "now"
        registry.Register(replica);
        var check = new LivenessCheck(registry);

        var result = await check.CheckHealthAsync(Context(check));

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Unhealthy_when_the_actor_loop_has_not_processed_anything_for_longer_than_five_seconds() {
        var registry = new RaftReplicaRegistry();
        // FakeTimeProvider defaults to a fixed point far in the past relative to real wall-clock
        // time — StartAsync stamps LastActorLoopActivity against it, so real DateTimeOffset.UtcNow
        // (what LivenessCheck itself compares against) already reads as hugely, deterministically
        // stale, with no need to wait out any real threshold.
        await using var replica = await StartAsync("node-1", new FakeTimeProvider());
        registry.Register(replica);
        var check = new LivenessCheck(registry);

        var result = await check.CheckHealthAsync(Context(check));

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("actor loop appears stuck");
    }

    [Fact]
    public async Task Healthy_for_an_idle_replica_without_peers_with_embedded_timeouts() {
        var registry = new RaftReplicaRegistry();
        // The timeouts of an embedded group: nothing forces an election or any real work, and a
        // heartbeat tick every 50 ms must be enough activity to stay healthy.
        var config = new RaftReplicaConfig(GroupId: "group-1", NodeId: "node-1", Peers: [],
            HeartbeatInterval: TimeSpan.FromMilliseconds(50), ElectionTimeoutMin: TimeSpan.FromMilliseconds(10), ElectionTimeoutMax: TimeSpan.FromMilliseconds(20));
        await using var replica = await StartAsync("node-1", config: config);
        registry.Register(replica);

        (await WaitUntilAsync(() => replica.IsLeader, TimeSpan.FromSeconds(2))).Should().BeTrue();
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var check = new LivenessCheck(registry);
        (await check.CheckHealthAsync(Context(check))).Status.Should().Be(HealthStatus.Healthy,
            "an idle embedded group must stay healthy — its periodic heartbeat is activity, nothing is stuck");
        LivenessCheck.StuckAfter.Should().Be(TimeSpan.FromSeconds(5));
    }

    static RaftReplicaConfig Config(string nodeId) =>
        new(GroupId: "group-1", NodeId: nodeId, Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

    static async Task<RaftReplica> StartAsync(string nodeId, TimeProvider? timeProvider = null, RaftReplicaConfig? config = null) {
        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config ?? Config(nodeId), timeProvider);
        await replica.StartAsync();
        return replica;
    }

    static Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout) =>
        ConditionPoller.WaitUntilAsync(condition, timeout, TimeSpan.FromMilliseconds(5));

    static HealthCheckContext Context(IHealthCheck check) =>
        new() { Registration = new HealthCheckRegistration("test", check, HealthStatus.Unhealthy, null) };
}
