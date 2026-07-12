using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;

namespace Remontoire.Messaging;

// Layer 1: every dependency is a plain Func test double, no RaftReplica/Raft harness needed — the
// direct payoff of §3.4's own dependency-graph correction (Remontoire.Messaging carries no
// reference to Remontoire.Raft/Remontoire.Sharding, and AckCheckpointer must not either).
public class AckCheckpointerTests {
    static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(1);

    [Fact]
    public async Task Never_proposes_when_not_leader() {
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0, 1, 2]);

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => false, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (null, 1), IsAdmissionPaused: () => false, timeProvider));

        await AdvanceAndSettleAsync(timeProvider);

        proposals.Should().BeEmpty();
    }

    [Fact]
    public async Task Never_proposes_when_admission_is_paused() {
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0, 1, 2]);

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (null, 1), IsAdmissionPaused: () => true, timeProvider));

        await AdvanceAndSettleAsync(timeProvider);

        proposals.Should().BeEmpty();
    }

    [Fact]
    public async Task Never_proposes_for_a_consumer_group_that_is_not_in_checkpoint_mode() {
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0, 1, 2]);

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => false,
            GetCheckpointThresholds: () => (null, 1), IsAdmissionPaused: () => false, timeProvider));

        await AdvanceAndSettleAsync(timeProvider);

        proposals.Should().BeEmpty();
    }

    [Fact]
    public async Task Never_proposes_when_the_watermark_has_not_advanced_since_the_last_checkpoint() {
        var (ackIndex, proposals, timeProvider) = Compose();
        // group-1 registered (RegisteredConsumerGroups sees it) but never acked anything — LowWatermark stays 0.
        ackIndex.GetOrCreate("group-1");

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (null, 1), IsAdmissionPaused: () => false, timeProvider));

        await AdvanceAndSettleAsync(timeProvider);

        proposals.Should().BeEmpty();
    }

    [Fact]
    public async Task Proposes_once_the_offset_count_threshold_is_reached() {
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0, 1, 2]); // LowWatermark -> 3

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (null, 2), IsAdmissionPaused: () => false, timeProvider));

        await AdvanceAndSettleAsync(timeProvider);

        proposals.Should().ContainSingle().Which.Should().Be(("group-1", 3UL));
    }

    [Fact]
    public async Task Does_not_propose_before_the_offset_count_threshold_is_reached() {
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0, 1, 2]); // LowWatermark -> 3, threshold needs 10

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (null, 10), IsAdmissionPaused: () => false, timeProvider));

        await AdvanceAndSettleAsync(timeProvider);

        proposals.Should().BeEmpty();
    }

    [Fact]
    public async Task Proposes_immediately_on_the_first_observed_advance_when_only_a_time_threshold_is_set() {
        // No prior checkpoint exists yet, so the "since last checkpoint" clock effectively starts
        // at the beginning of time — the first observed advance is always immediately due.
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0]);

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (TimeSpan.FromSeconds(10), null), IsAdmissionPaused: () => false, timeProvider));

        await AdvanceAndSettleAsync(timeProvider);

        proposals.Should().ContainSingle().Which.Should().Be(("group-1", 1UL));
    }

    [Fact]
    public async Task Waits_for_the_time_threshold_before_a_second_checkpoint() {
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0]);

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (TimeSpan.FromSeconds(10), null), IsAdmissionPaused: () => false, timeProvider));

        await AdvanceAndSettleAsync(timeProvider); // first checkpoint, immediate
        proposals.Should().ContainSingle();

        ackIndex.ApplyLocal("group-1", [1, 2]); // LowWatermark -> 3, but only 5s pass — not due yet
        for (var i = 0; i < 5; i++)
            await AdvanceAndSettleAsync(timeProvider);
        proposals.Should().ContainSingle("5 seconds have passed, the 10-second interval is not yet due");

        for (var i = 0; i < 6; i++) // 11 seconds total since the first checkpoint
            await AdvanceAndSettleAsync(timeProvider);
        proposals.Should().HaveCount(2);
        proposals.Last().Should().Be(("group-1", 3UL));
    }

    [Fact]
    public async Task Eventually_checkpoints_even_when_no_threshold_is_configured_at_all() {
        // (null, null) must not mean "never" — otherwise a checkpoint-mode group that never gets
        // an explicit SetStreamCheckpointInterval never checkpoints at all: CommittedWatermark
        // stays 0 forever, permanently blocking pruning if the group is mandatory, and losing
        // ALL ack progress (not just "up to one interval's worth") on any failover.
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0]);

        await using var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (null, null), IsAdmissionPaused: () => false, timeProvider));

        await AdvanceAndSettleAsync(timeProvider);

        proposals.Should().ContainSingle("no explicit threshold must still fall back to a safe default interval, not disable checkpointing entirely");
    }

    [Fact]
    public async Task DisposeAsync_stops_the_loop() {
        var (ackIndex, proposals, timeProvider) = Compose();
        ackIndex.ApplyLocal("group-1", [0]);
        var checkpointer = new AckCheckpointer(new AckCheckpointerOptions(
            ackIndex, Propose(proposals), IsLeader: () => true, IsCheckpointMode: _ => true,
            GetCheckpointThresholds: () => (null, 1), IsAdmissionPaused: () => false, timeProvider));
        await AdvanceAndSettleAsync(timeProvider);
        proposals.Should().ContainSingle();

        await checkpointer.DisposeAsync();
        ackIndex.ApplyLocal("group-1", [1, 2, 3, 4, 5]);
        timeProvider.Advance(TickInterval * 5);
        await Task.Delay(50);

        proposals.Should().ContainSingle("the loop must not tick again after disposal");
    }

    static (AckIndex AckIndex, ConcurrentQueue<(string Group, ulong Watermark)> Proposals, FakeTimeProvider TimeProvider) Compose() =>
        (new AckIndex(), new ConcurrentQueue<(string, ulong)>(), new FakeTimeProvider());

    static Func<string, ulong, CancellationToken, Task> Propose(ConcurrentQueue<(string Group, ulong Watermark)> proposals) =>
        (consumerGroup, watermark, _) => {
            proposals.Enqueue((consumerGroup, watermark));
            return Task.CompletedTask;
        };

    // Advances one tick and gives the background loop a short, real-time window to run the
    // continuation FakeTimeProvider's timer firing scheduled — the same "advance, then let it
    // settle" shape SimulatedCluster.StepAsync uses for the same underlying reason. The leading
    // delay matters just as much as the trailing one: Task.Run schedules the loop onto the thread
    // pool asynchronously, so without it a call right after construction can race ahead of the
    // loop even reaching its first Task.Delay subscription, and Advance finds nothing to fire.
    static async Task AdvanceAndSettleAsync(FakeTimeProvider timeProvider) {
        await Task.Delay(20);
        timeProvider.Advance(TickInterval);
        await Task.Delay(50);
    }
}
