using Remontoire.Storage;

namespace Remontoire.Raft;

/// <summary>
/// The deterministic-run cousin of Jepsen-style fault-injection testing: many seeded runs, each
/// firing random crashes/restarts/partitions/proposals at a 3-node cluster, with
/// <see cref="RaftInvariantChecker"/> asserting all five safety properties after every single
/// step. A failure names its seed — re-running that one seed reproduces it closely (modulo the
/// real-thread-scheduling caveat already noted on <see cref="SimulatedCluster"/>).
/// </summary>
public class RaftInvariantFuzzTests {
    const int StepsPerRun = 300;

    public static IEnumerable<object[]> Seeds() {
        for (var seed = 101; seed < 201; seed++)
            yield return [seed];
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task A_randomised_run_never_violates_a_safety_invariant(int seed) {
        var networkOptions = new SimulationOptions(
            MessageDropProbability: 0.1, MinDelay: TimeSpan.FromMilliseconds(1), MaxDelay: TimeSpan.FromMilliseconds(40), AllowReorder: true);
        await using var cluster = new SimulatedCluster(nodeCount: 3, seed, networkOptions);
        await cluster.StartAllAsync();

        var checker = new RaftInvariantChecker(cluster);
        var fuzzRandom = new Random(seed);
        var allNodeIds = cluster.Replicas.Keys.ToArray();
        var pendingProposals = new List<Task>();

        for (var step = 0; step < StepsPerRun; step++) {
            try {
                await FuzzOneActionAsync(cluster, allNodeIds, fuzzRandom, step, pendingProposals);
            } catch (NotLeaderException) {
                // Expected: e.g. the replica picked as "the leader" a moment ago stepped down
                // (lost an election, got partitioned, or crashed) before the action landed.
            } catch (ObjectDisposedException) {
                // Expected: the picked replica crashed between selection and the action landing.
            }

            await cluster.StepAsync(TimeSpan.FromMilliseconds(20));

            try {
                await checker.AssertAsync();
            } catch (RaftInvariantViolationException ex) {
                throw new RaftInvariantViolationException($"[seed {seed}, step {step}] {ex.Message}");
            }
        }

        // Settle before the test ends: heal any partition and restart any crashed node, so a
        // full, healthy 3-node cluster exists again — otherwise a still-pending proposal (its
        // leader stuck without quorum) could wait forever for virtual time nothing advances
        // anymore. No new fuzz actions here, no invariant checks — this is cleanup, not scenario.
        cluster.Heal();
        foreach (var nodeId in allNodeIds)
            if (!cluster.Replicas.ContainsKey(nodeId))
                await cluster.RestartAsync(nodeId);

        for (var extraStep = 0; extraStep < 100 && pendingProposals.Any(proposal => !proposal.IsCompleted); extraStep++)
            await cluster.StepAsync(TimeSpan.FromMilliseconds(20));

        foreach (var proposal in pendingProposals)
            await proposal; // each already swallows its own outcome (see IgnoreOutcome) — never throws
    }

    static async Task FuzzOneActionAsync(SimulatedCluster cluster, string[] allNodeIds, Random fuzzRandom, int step, List<Task> pendingProposals) {
        switch (fuzzRandom.Next(10)) {
            case 0: {
                var leader = cluster.Replicas.Values.FirstOrDefault(replica => replica.IsLeader);
                if (leader is not null) {
                    var request = new AppendRequest(BitConverter.GetBytes(step), [], "payload"u8.ToArray());
                    pendingProposals.Add(IgnoreOutcome(leader.ProposeAsync(request).AsTask()));
                }
                break;
            }
            case 1: {
                if (cluster.Replicas.Count > 0) {
                    var nodeId = cluster.Replicas.Keys.ElementAt(fuzzRandom.Next(cluster.Replicas.Count));
                    await cluster.CrashAsync(nodeId);
                }
                break;
            }
            case 2: {
                var crashedNodeId = allNodeIds.FirstOrDefault(nodeId => !cluster.Replicas.ContainsKey(nodeId));
                if (crashedNodeId is not null)
                    await cluster.RestartAsync(crashedNodeId);
                break;
            }
            case 3:
                cluster.Partition([allNodeIds[0]], [allNodeIds[1], allNodeIds[2]]);
                break;
            case 4:
                cluster.Heal();
                break;
            default:
                break; // most steps: no action, just let virtual time and in-flight messages advance
        }
    }

    // Wraps a proposal so its eventual success or failure never surfaces as an unobserved task
    // exception — the checker, not the proposal's own outcome, is what this test cares about.
    static async Task IgnoreOutcome(Task task) {
        try {
            await task;
        } catch {
            // ignored — expected outcomes include NotLeaderException (stepped down/crashed) and
            // a hang-free failure from a partition that never heals before the run ends.
        }
    }
}
