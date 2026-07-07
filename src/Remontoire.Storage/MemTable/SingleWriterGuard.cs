namespace Remontoire.Storage;

/// <summary>
/// A cheap, non-blocking guard that detects — and throws on — concurrent re-entry, instead of
/// silently corrupting state that assumes a single writer. Not a lock: nothing ever waits — a
/// second, concurrent <see cref="Enter"/> call throws immediately rather than blocking until
/// the first one exits.
/// </summary>
sealed class SingleWriterGuard {
    int _state;

    /// <summary>
    /// Marks entry. Dispose the returned <see cref="Scope"/> to mark exit — typically via
    /// <c>using</c>. Throws <see cref="InvalidOperationException"/> if already entered.
    /// </summary>
    public Scope Enter() =>
        Interlocked.Exchange(ref _state, 1) != 0
            ? throw new InvalidOperationException($"Concurrent entry into a {nameof(SingleWriterGuard)} is not supported.")
            : new Scope(this);

    /// <summary>
    /// Marks exit from a <see cref="SingleWriterGuard"/> when disposed.
    /// </summary>
    public readonly struct Scope(SingleWriterGuard guard) : IDisposable {
        /// <inheritdoc />
        public void Dispose() =>
            Volatile.Write(ref guard._state, 0);
    }
}
