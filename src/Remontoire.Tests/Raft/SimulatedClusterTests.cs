using FluentAssertions;
using Remontoire.Storage;

namespace Remontoire.Raft;

public class SimulatedClusterTests {
    static SimulationOptions ReliableNetwork => new(
        MessageDropProbability: 0, MinDelay: TimeSpan.FromMilliseconds(1), MaxDelay: TimeSpan.FromMilliseconds(5), AllowReorder: true);

    // Steps the cluster forward until condition() is true or the step budget runs out — the
    // simulation-driven equivalent of the layer-1 tests' real-time polling helper. 500 steps of
    // 20 ms is 10 s of virtual time, comfortably more than several election-timeout rounds
    // (150-300 ms each) at the timings SimulatedCluster configures its replicas with.
    static async Task<bool> RunUntilAsync(SimulatedCluster cluster, Func<bool> condition, int maxSteps = 500, TimeSpan? step = null) {
        var increment = step ?? TimeSpan.FromMilliseconds(20);
        for (var i = 0; i < maxSteps; i++) {
            if (condition())
                return true;
            await cluster.StepAsync(increment);
        }
        return condition();
    }

    static RaftReplica? CurrentLeader(SimulatedCluster cluster) =>
        cluster.Replicas.Values.FirstOrDefault(replica => replica.IsLeader);

    static AppendRequest SampleRequest(string key = "key") =>
        new(System.Text.Encoding.UTF8.GetBytes(key), [], "payload"u8.ToArray());

    [Fact]
    public async Task A_three_node_cluster_elects_exactly_one_leader() {
        await using var cluster = new SimulatedCluster(nodeCount: 3, seed: 1, ReliableNetwork);
        await cluster.StartAllAsync();

        var elected = await RunUntilAsync(cluster, () => CurrentLeader(cluster) is not null);

        elected.Should().BeTrue("a 3-node cluster with a reliable network must elect a leader");
        cluster.Replicas.Values.Count(replica => replica.IsLeader).Should().Be(1);
    }

    [Fact]
    public async Task A_proposal_commits_and_every_replica_eventually_sees_it_committed() {
        await using var cluster = new SimulatedCluster(nodeCount: 3, seed: 2, ReliableNetwork);
        await cluster.StartAllAsync();
        await RunUntilAsync(cluster, () => CurrentLeader(cluster) is not null);
        var leader = CurrentLeader(cluster)!;

        var proposeTask = leader.ProposeAsync(SampleRequest()).AsTask();
        (await RunUntilAsync(cluster, () => proposeTask.IsCompleted)).Should().BeTrue("the proposal must commit on a healthy 3-node cluster");

        var result = await proposeTask;
        result.LogicalOffset.Should().Be(0);

        foreach (var (nodeId, replica) in cluster.Replicas)
            (await RunUntilAsync(cluster, () => replica.CommitIndex >= result.RaftIndex))
                .Should().BeTrue($"'{nodeId}' should eventually catch up to the committed index");
    }

    [Fact]
    public async Task Leader_crash_triggers_re_election_and_the_new_leader_keeps_the_committed_entry() {
        await using var cluster = new SimulatedCluster(nodeCount: 3, seed: 3, ReliableNetwork);
        await cluster.StartAllAsync();
        await RunUntilAsync(cluster, () => CurrentLeader(cluster) is not null);

        var firstLeader = CurrentLeader(cluster)!;
        var firstLeaderNodeId = cluster.Replicas.First(pair => pair.Value == firstLeader).Key;

        var proposeTask = firstLeader.ProposeAsync(SampleRequest()).AsTask();
        (await RunUntilAsync(cluster, () => proposeTask.IsCompleted)).Should().BeTrue();
        var committedResult = await proposeTask;

        await cluster.CrashAsync(firstLeaderNodeId);

        var reElected = await RunUntilAsync(cluster, () => cluster.Replicas.Values.Any(replica => replica.IsLeader));
        reElected.Should().BeTrue("the two surviving nodes must elect a new leader");
        cluster.Replicas.ContainsKey(firstLeaderNodeId).Should().BeFalse("the crashed node stays down until explicitly restarted");

        var newLeader = CurrentLeader(cluster)!;
        (await RunUntilAsync(cluster, () => newLeader.CommitIndex >= committedResult.RaftIndex))
            .Should().BeTrue("the entry committed before the crash must survive it");
    }

    [Fact]
    public async Task A_restarted_node_catches_up_with_what_it_missed_while_down() {
        await using var cluster = new SimulatedCluster(nodeCount: 3, seed: 5, ReliableNetwork);
        await cluster.StartAllAsync();
        await RunUntilAsync(cluster, () => CurrentLeader(cluster) is not null);

        var leader = CurrentLeader(cluster)!;
        var leaderNodeId = cluster.Replicas.First(pair => pair.Value == leader).Key;
        var followerNodeId = cluster.Replicas.Keys.First(nodeId => nodeId != leaderNodeId);

        await cluster.CrashAsync(followerNodeId);

        var proposeTask = leader.ProposeAsync(SampleRequest()).AsTask();
        (await RunUntilAsync(cluster, () => proposeTask.IsCompleted)).Should().BeTrue("the remaining two nodes (leader + one follower) still satisfy quorum");
        var committedResult = await proposeTask;

        await cluster.RestartAsync(followerNodeId);

        var caughtUp = await RunUntilAsync(cluster, () => cluster.Replicas[followerNodeId].CommitIndex >= committedResult.RaftIndex);
        caughtUp.Should().BeTrue("the restarted node must replay/replicate up to what it missed while it was down");
    }

    [Fact]
    public async Task A_minority_partition_cannot_commit_while_the_majority_island_does() {
        await using var cluster = new SimulatedCluster(nodeCount: 3, seed: 4, ReliableNetwork);
        await cluster.StartAllAsync();
        await RunUntilAsync(cluster, () => CurrentLeader(cluster) is not null);

        var nodeIds = cluster.Replicas.Keys.ToArray();
        var minorityNodeId = nodeIds[0];
        var majorityNodeIds = new[] { nodeIds[1], nodeIds[2] };
        cluster.Partition([minorityNodeId], majorityNodeIds);

        // The majority island (2 of 3 — still a quorum on its own) must be able to have/elect a
        // leader regardless of whether the original leader ended up on this side or the minority.
        var majorityHasLeader = await RunUntilAsync(cluster,
            () => majorityNodeIds.Any(nodeId => cluster.Replicas[nodeId].IsLeader));
        majorityHasLeader.Should().BeTrue("2 of 3 nodes already satisfy quorum and must be able to make progress");

        var majorityLeader = cluster.Replicas[majorityNodeIds.First(nodeId => cluster.Replicas[nodeId].IsLeader)];
        var proposeTask = majorityLeader.ProposeAsync(SampleRequest()).AsTask();

        (await RunUntilAsync(cluster, () => proposeTask.IsCompleted))
            .Should().BeTrue("the majority island alone already satisfies quorum and must be able to commit");

        cluster.Heal();
    }
}
