namespace Remontoire.Raft;

/// <summary>
/// An in-memory <see cref="IRaftStateStore"/> — "durable" only for the lifetime of the test.
/// Justifies <see cref="IRaftStateStore"/>'s existence alongside <c>FileRaftStateStore</c>.
/// </summary>
sealed class InMemoryRaftStateStore : IRaftStateStore {
    RaftPersistentState _state;

    public ValueTask<RaftPersistentState> LoadAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(_state);

    public ValueTask SaveAsync(RaftPersistentState state, CancellationToken cancellationToken = default) {
        _state = state;
        return ValueTask.CompletedTask;
    }
}
