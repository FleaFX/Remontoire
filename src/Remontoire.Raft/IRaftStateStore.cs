namespace Remontoire.Raft;

/// <summary>
/// Durable storage for <see cref="RaftPersistentState"/>. <see cref="SaveAsync"/> must be
/// crash-atomic (temp + fsync + rename) and must not return before the state is on disk.
/// </summary>
public interface IRaftStateStore {
    /// <summary>
    /// Loads the saved state, or term 0 / no vote when nothing was saved yet.
    /// </summary>
    ValueTask<RaftPersistentState> LoadAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Durably persists <paramref name="state"/> before returning.
    /// </summary>
    ValueTask SaveAsync(RaftPersistentState state, CancellationToken cancellationToken = default);
}
