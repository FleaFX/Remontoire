using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Raft.V1;

namespace Remontoire.Server.HealthChecks;

public class RaftReadinessCheckTests {
    [Fact]
    public async Task Unhealthy_when_no_group_is_hosted_at_all() {
        var check = new RaftReadinessCheck(new RaftReplicaRegistry());

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.Should().Be(HealthStatus.Unhealthy);
    }

    [Fact]
    public async Task Healthy_when_the_only_hosted_group_is_this_node_s_leader() {
        var registry = new RaftReplicaRegistry();
        await using var replica = await StartSingleNodeLeaderAsync("group-1", "node-1");
        registry.Register(replica);
        var check = new RaftReadinessCheck(registry);

        (await check.CheckHealthAsync(new HealthCheckContext())).Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Unhealthy_when_the_only_hosted_group_has_never_had_leader_contact() {
        var registry = new RaftReplicaRegistry();
        var config = new RaftReplicaConfig(GroupId: "group-1", NodeId: "node-1", Peers: [new RaftGroupMember("node-2", new Uri("https://node-2.local"))],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));
        await using var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync(); // stays a plain follower — no election, no AppendEntries ever received
        registry.Register(replica);
        var check = new RaftReadinessCheck(registry);

        (await check.CheckHealthAsync(new HealthCheckContext())).Status.Should().Be(HealthStatus.Unhealthy);
    }

    // The core OR-aggregation regression test: one stuck group must never drag a node with an
    // otherwise-healthy group into Unhealthy.
    [Fact]
    public async Task Healthy_when_at_least_one_hosted_group_has_active_leader_contact_even_if_another_never_had_any() {
        var registry = new RaftReplicaRegistry();

        await using var stuckReplica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(),
            new RaftReplicaConfig(GroupId: "group-stuck", NodeId: "node-1", Peers: [new RaftGroupMember("node-2", new Uri("https://node-2.local"))],
                HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11)));
        await stuckReplica.StartAsync();
        registry.Register(stuckReplica);

        await using var contactedReplica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(),
            new RaftReplicaConfig(GroupId: "group-ok", NodeId: "node-1", Peers: [new RaftGroupMember("node-2", new Uri("https://node-2.local"))],
                HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11)));
        await contactedReplica.StartAsync();
        var reply = new TaskCompletionSource<AppendEntriesResponse>();
        contactedReplica.TryPost(new AppendEntriesReceived(
            new AppendEntriesRequest { GroupId = "group-ok", Term = 1, LeaderId = "node-2", PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommit = 0 }, reply));
        await contactedReplica.DrainAsync();
        (await reply.Task).Success.Should().BeTrue("sanity check on the setup");
        registry.Register(contactedReplica);

        var check = new RaftReadinessCheck(registry);
        (await check.CheckHealthAsync(new HealthCheckContext())).Status.Should().Be(HealthStatus.Healthy);
    }

    static async Task<RaftReplica> StartSingleNodeLeaderAsync(string groupId, string nodeId) {
        var config = new RaftReplicaConfig(GroupId: groupId, NodeId: nodeId, Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));
        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync();
        replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node group -> immediate leader
        await replica.DrainAsync();
        return replica;
    }
}
