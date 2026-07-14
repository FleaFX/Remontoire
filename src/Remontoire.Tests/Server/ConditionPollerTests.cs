using FluentAssertions;

namespace Remontoire.Server;

public class ConditionPollerTests {
    public class WaitUntilAsync {
        [Fact]
        public async Task Returns_true_immediately_when_the_condition_is_already_true() {
            var result = await ConditionPoller.WaitUntilAsync(() => true, TimeSpan.FromSeconds(1));

            result.Should().BeTrue();
        }

        [Fact]
        public async Task Returns_true_once_the_condition_becomes_true_during_a_later_poll() {
            var attempts = 0;

            var result = await ConditionPoller.WaitUntilAsync(
                () => ++attempts >= 3, TimeSpan.FromSeconds(2), pollInterval: TimeSpan.FromMilliseconds(10));

            result.Should().BeTrue();
            attempts.Should().Be(3);
        }

        [Fact]
        public async Task Returns_false_when_the_condition_never_becomes_true_before_the_timeout() {
            var result = await ConditionPoller.WaitUntilAsync(
                () => false, TimeSpan.FromMilliseconds(50), pollInterval: TimeSpan.FromMilliseconds(10));

            result.Should().BeFalse();
        }

        [Fact]
        public async Task Re_evaluates_the_condition_one_last_time_after_the_deadline_elapses() {
            // The while loop's own deadline check can lose a race against a condition that flips
            // true right as the timeout elapses — the trailing re-evaluation after the loop exists
            // specifically to still catch that case instead of reporting a false timeout.
            var deadline = DateTime.UtcNow + TimeSpan.FromMilliseconds(30);

            var result = await ConditionPoller.WaitUntilAsync(
                () => DateTime.UtcNow >= deadline, TimeSpan.FromMilliseconds(30), pollInterval: TimeSpan.FromMilliseconds(100));

            result.Should().BeTrue();
        }

        [Fact]
        public async Task Stops_polling_when_the_cancellation_token_is_cancelled() {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var act = () => ConditionPoller.WaitUntilAsync(() => false, TimeSpan.FromSeconds(5), cancellationToken: cts.Token);

            await act.Should().ThrowAsync<TaskCanceledException>();
        }
    }
}
