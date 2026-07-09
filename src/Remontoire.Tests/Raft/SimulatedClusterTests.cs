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

    [Fact]
    public async Task A_node_that_missed_a_compacted_prefix_catches_up_via_InstallSnapshot() {
        var options = ReliableNetwork with { SnapshotThresholdEntries = 2 };
        await using var cluster = new SimulatedCluster(nodeCount: 3, seed: 6, options);
        await cluster.StartAllAsync();
        await RunUntilAsync(cluster, () => CurrentLeader(cluster) is not null);

        var leader = CurrentLeader(cluster)!;
        var leaderNodeId = cluster.Replicas.First(pair => pair.Value == leader).Key;
        var laggingNodeId = cluster.Replicas.Keys.First(nodeId => nodeId != leaderNodeId);

        await cluster.CrashAsync(laggingNodeId);

        // Enough proposals for the tail to exceed the (very low) snapshot threshold — the
        // surviving two nodes still satisfy quorum without the crashed one.
        ProposeResult? lastResult = null;
        for (var i = 0; i < 6; i++) {
            var proposeTask = leader.ProposeAsync(SampleRequest($"key-{i}")).AsTask();
            (await RunUntilAsync(cluster, () => proposeTask.IsCompleted)).Should().BeTrue();
            lastResult = await proposeTask;
        }

        // Confirms the leader actually compacted away the prefix the crashed node needs —
        // structurally, that makes InstallSnapshot the only possible way it can ever catch up:
        // SendAppendEntriesAsync refuses to serve anything at or below raftLog.SnapshotIndex.
        var compacted = await RunUntilAsync(cluster, () => cluster.Logs[leaderNodeId].SnapshotIndex > 0);
        compacted.Should().BeTrue("the tail comfortably exceeds the threshold after 6 proposals");

        await cluster.RestartAsync(laggingNodeId);

        var caughtUp = await RunUntilAsync(cluster, () => cluster.Replicas[laggingNodeId].CommitIndex >= lastResult!.Value.RaftIndex);
        caughtUp.Should().BeTrue("InstallSnapshot must bring the restarted node's log base forward, since normal AppendEntries no longer can");
    }

    [Fact]
    public async Task A_config_change_commits_and_the_remaining_members_reflect_the_new_membership() {
        await using var cluster = new SimulatedCluster(nodeCount: 3, seed: 7, ReliableNetwork);
        await cluster.StartAllAsync();

        // Captured atomically within the polling predicate itself, unlike the CurrentLeader(cluster)
        // + separate by-reference lookup pattern used elsewhere in this file: this test's extra
        // CrashAsync call below widens the real-thread scheduling window enough that a second,
        // independent re-fetch of "the" leader occasionally observes none (a step-down raced in
        // between), which this closes by never re-fetching at all.
        (string LeaderNodeId, RaftReplica Leader)? leaderPair = null;
        await RunUntilAsync(cluster, () => {
            var found = cluster.Replicas.FirstOrDefault(pair => pair.Value.IsLeader);
            if (found.Value is null)
                return false;
            leaderPair = (found.Key, found.Value);
            return true;
        });
        var (leaderNodeId, leader) = leaderPair!.Value;

        var nodeIds = cluster.Replicas.Keys.ToArray();
        var remainingFollowerNodeId = nodeIds.First(nodeId => nodeId != leaderNodeId);
        var removedNodeId = nodeIds.First(nodeId => nodeId != leaderNodeId && nodeId != remainingFollowerNodeId);

        var newMembership = new[] {
            new RaftGroupMember(leaderNodeId, new Uri($"https://{leaderNodeId}.simulated")),
            new RaftGroupMember(remainingFollowerNodeId, new Uri($"https://{remainingFollowerNodeId}.simulated")),
        };

        // Crashed before the removal is proposed, not after: once out of the group it stops
        // receiving heartbeats and, without Pre-Vote (deliberately out of scope), would otherwise
        // time out and disrupt the surviving members with an election under its own stale peer
        // list — the documented, deferred-to-Pre-Vote disruptor-node limitation. A real operator
        // removing a node sequences it the same way: stop it, then propose the removal.
        await cluster.CrashAsync(removedNodeId);

        var configChangeTask = leader.ProposeConfigChangeAsync(newMembership).AsTask();
        (await RunUntilAsync(cluster, () => configChangeTask.IsCompleted))
            .Should().BeTrue($"'{removedNodeId}' is dropped from quorum the moment the change is appended, per the paper's effective-on-append rule");
        await configChangeTask;

        leader.ActiveConfiguration.Select(m => m.NodeId).Should().BeEquivalentTo([remainingFollowerNodeId]);
        (await RunUntilAsync(cluster, () =>
                cluster.Replicas[remainingFollowerNodeId].ActiveConfiguration.Select(m => m.NodeId).SequenceEqual([leaderNodeId])))
            .Should().BeTrue($"'{remainingFollowerNodeId}' must pick up the same membership change via ordinary replication over the wire");

        // The shrunk group must still be able to make progress afterward — proves the change was
        // more than cosmetic on the surviving members.
        var proposeTask = leader.ProposeAsync(SampleRequest()).AsTask();
        (await RunUntilAsync(cluster, () => proposeTask.IsCompleted)).Should().BeTrue();
        var result = await proposeTask;

        (await RunUntilAsync(cluster, () => cluster.Replicas[remainingFollowerNodeId].CommitIndex >= result.RaftIndex))
            .Should().BeTrue();
    }
}
