using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Time.Testing;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;

namespace Remontoire.Server.HealthChecks;

public class RaftLivenessCheckTests {
    [Fact]
    public async Task Healthy_when_no_replica_is_registered() {
        var check = new RaftLivenessCheck(new RaftReplicaRegistry());

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Healthy_when_the_actor_loop_started_recently() {
        var registry = new RaftReplicaRegistry();
        await using var replica = await StartAsync("node-1"); // real TimeProvider.System — just stamped "now"
        registry.Register(replica);
        var check = new RaftLivenessCheck(registry);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Unhealthy_when_the_actor_loop_has_not_processed_anything_within_a_configured_multiple_of_ElectionTimeoutMax() {
        var registry = new RaftReplicaRegistry();
        // FakeTimeProvider defaults to a fixed point far in the past relative to real wall-clock
        // time — StartAsync stamps LastActorLoopActivity against it, so real DateTimeOffset.UtcNow
        // (what RaftLivenessCheck itself compares against) already reads as hugely, deterministically
        // stale, with no need to wait out any real threshold.
        await using var replica = await StartAsync("node-1", new FakeTimeProvider());
        registry.Register(replica);
        var check = new RaftLivenessCheck(registry);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("actor loop appears stuck");
    }

    static RaftReplicaConfig Config(string nodeId) =>
        new(GroupId: "group-1", NodeId: nodeId, Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

    static async Task<RaftReplica> StartAsync(string nodeId, TimeProvider? timeProvider = null) {
        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), Config(nodeId), timeProvider);
        await replica.StartAsync();
        return replica;
    }
}
