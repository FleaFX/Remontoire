namespace Remontoire.Server;

/// <summary>
/// Polls a local, in-memory condition until it becomes true or a timeout elapses — the same
/// wandklok-polling idiom every test harness in this project already hand-rolls its own copy of,
/// promoted to production code for the first time by Cutover's own bounded pause->propose->observe
/// sequence. Deliberately generic, not specialized to any one condition shape (e.g. ShardAssignmentTable):
/// nothing about polling itself depends on what's being polled for.
/// </summary>
public static class ConditionPoller {
    /// <summary>
    /// Polls <paramref name="condition"/> every <paramref name="pollInterval"/> until it returns
    /// <see langword="true"/> or <paramref name="timeout"/> elapses. Always re-evaluates
    /// <paramref name="condition"/> once more after the deadline, so a condition that flips true
    /// right as the timeout elapses is still reported as a success, not a false timeout.
    /// </summary>
    /// <param name="condition">The predicate to poll.</param>
    /// <param name="timeout">How long to keep polling before giving up.</param>
    /// <param name="pollInterval">The delay between polls — defaults to 20 milliseconds.</param>
    /// <param name="cancellationToken">Cancels the wait; does not affect the trailing re-evaluation.</param>
    /// <returns><see langword="true"/> if <paramref name="condition"/> became true in time, <see langword="false"/> otherwise.</returns>
    public static async Task<bool> WaitUntilAsync(
        Func<bool> condition, TimeSpan timeout, TimeSpan? pollInterval = null, CancellationToken cancellationToken = default) {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(20);
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline) {
            if (condition())
                return true;
            await Task.Delay(interval, cancellationToken);
        }
        return condition();
    }
}
