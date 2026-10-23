namespace Remontoire.Engine;

/// <summary>
/// One open <see cref="IRemontoireConsumer.ConsumeAsync"/> stream per test, with its own
/// cancellation source: a read that times out cancels the stream first, awaits the in-flight
/// <c>MoveNextAsync</c>, and only then disposes the iterator — disposing it while the
/// <c>MoveNextAsync</c> is still running throws <see cref="NotSupportedException"/> and hides
/// the real failure.
/// </summary>
internal sealed class ConsumeStream : IAsyncDisposable {
    readonly CancellationTokenSource _cts;
    readonly IAsyncEnumerator<RemontoireMessage> _messages;

    /// <summary>
    /// Opens <paramref name="stream"/> with the cancellation source this helper owns.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    public ConsumeStream(IAsyncEnumerable<RemontoireMessage> stream) {
        _cts = new CancellationTokenSource();
        _messages = stream.GetAsyncEnumerator(_cts.Token);
    }

    /// <summary>
    /// The stream's next message, or a <see cref="TimeoutException"/> after 10 s.
    /// </summary>
    public async Task<RemontoireMessage> NextAsync() {
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var moveNext = _messages.MoveNextAsync().AsTask();
        try {
            if (await moveNext.WaitAsync(timeoutCts.Token))
                return _messages.Current;
        } catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested) {
            _cts.Cancel();
            try {
                await moveNext;
            } catch (OperationCanceledException) {
                // The cancelled stream's own unwinding ends here.
            }

            await _messages.DisposeAsync();
            throw new TimeoutException("No message arrived within 10 s.");
        }

        throw new TimeoutException("No message arrived within 10 s.");
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        await _messages.DisposeAsync();
        _cts.Dispose();
    }
}
