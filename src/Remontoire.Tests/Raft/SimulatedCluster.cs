using Microsoft.Extensions.Time.Testing;

namespace Remontoire.Raft;

/// <summary>
/// A deterministic-ish in-memory network of <see cref="RaftReplica"/>s. All delivery decisions
/// (drop, delay, reorder, partition) come from one seeded <see cref="Random"/>; a failing run is
/// reproduced closely by re-running its logged seed — "closely", not bit-for-bit, since real
/// background tasks (not a single-threaded discrete-event loop) still drive delivery, so the
/// exact interleaving of concurrent sends can vary with real thread scheduling even for the same
/// seed. Time is virtual (<see cref="FakeTimeProvider"/>) — many simulated elections cost
/// milliseconds of wall-clock time, not the real election-timeout duration.
/// </summary>
sealed class SimulatedCluster : IAsyncDisposable {
    readonly FakeTimeProvider _timeProvider = new();
    readonly Random _random;
    readonly SimulationOptions _options;
    readonly RaftGroupMember[] _members;
    readonly Dictionary<string, IRaftLog> _logs = [];
    readonly Dictionary<string, IRaftStateStore> _stateStores = [];
    readonly Dictionary<(string From, string To), DateTimeOffset> _lastScheduledArrival = [];
    readonly Lock _randomLock = new();
    readonly Lock _arrivalLock = new();

    string[][] _islands;

    public SimulatedCluster(int nodeCount, int seed, SimulationOptions options) {
        _random = new Random(seed);
        _options = options;

        var nodeIds = Enumerable.Range(1, nodeCount).Select(i => $"node-{i}").ToArray();
        _members = nodeIds.Select(id => new RaftGroupMember(id, new Uri($"https://{id}.simulated"))).ToArray();
        _islands = [nodeIds];

        Replicas = new Dictionary<string, RaftReplica>();
        foreach (var nodeId in nodeIds)
            CreateReplica(nodeId);
    }

    /// <summary>
    /// The currently-running replicas, keyed by node ID. A crashed node is absent until restarted.
    /// </summary>
    public Dictionary<string, RaftReplica> Replicas { get; }

    /// <summary>
    /// Every node's simulated durable log, keyed by node ID — including crashed nodes' (a log
    /// survives a crash; only the actor state doesn't). Read-only access for invariant-checking;
    /// nothing outside <see cref="RaftReplica"/> itself should ever write through this.
    /// </summary>
    public IReadOnlyDictionary<string, IRaftLog> Logs => _logs;

    void CreateReplica(string nodeId) {
        if (!_logs.TryGetValue(nodeId, out var log))
            _logs[nodeId] = log = new InMemoryRaftLog();
        if (!_stateStores.TryGetValue(nodeId, out var stateStore))
            _stateStores[nodeId] = stateStore = new InMemoryRaftStateStore();

        var peers = _members.Where(member => member.NodeId != nodeId).ToArray();
        var config = new RaftReplicaConfig(
            GroupId: "group-1", NodeId: nodeId, Peers: peers,
            HeartbeatInterval: TimeSpan.FromMilliseconds(50),
            ElectionTimeoutMin: TimeSpan.FromMilliseconds(150),
            ElectionTimeoutMax: TimeSpan.FromMilliseconds(300),
            SnapshotThresholdEntries: _options.SnapshotThresholdEntries ?? 10_000);

        // No real segments exist at this layer (InMemoryRaftLog) — both delegates are trivial
        // no-ops, only exercised at all when SnapshotThresholdEntries is set.
        Func<ulong, CancellationToken, Task<IReadOnlyList<string>>>? prepareSnapshot = _options.SnapshotThresholdEntries is null
            ? null : (_, _) => Task.FromResult<IReadOnlyList<string>>([]);
        Func<IReadOnlyList<string>, ulong, CancellationToken, Task>? installSnapshot = _options.SnapshotThresholdEntries is null
            ? null : (_, _, _) => Task.CompletedTask;

        Replicas[nodeId] = new RaftReplica(stateStore, log, new SimulatedTransport(nodeId, this), config, _timeProvider,
            prepareSnapshot: prepareSnapshot, installSnapshot: installSnapshot);
    }

    /// <summary>
    /// Starts every replica. Call once, before the first <see cref="StepAsync"/>.
    /// </summary>
    public Task StartAllAsync(CancellationToken cancellationToken = default) =>
        Task.WhenAll(Replicas.Values.Select(replica => replica.StartAsync(cancellationToken)));

    /// <summary>
    /// Splits the cluster into isolated islands; messages between nodes in different islands are
    /// dropped, whether already in flight or not yet sent.
    /// </summary>
    public void Partition(params string[][] islands) => _islands = islands;

    /// <summary>
    /// Removes any active partition — every node can reach every other node again.
    /// </summary>
    public void Heal() => _islands = [_members.Select(member => member.NodeId).ToArray()];

    /// <summary>
    /// Crashes a node: stops its actor loop and discards its volatile state. Its
    /// <see cref="IRaftLog"/>/<see cref="IRaftStateStore"/> — the simulated durable storage —
    /// survive untouched for a later <see cref="RestartAsync"/>.
    /// </summary>
    public async Task CrashAsync(string nodeId) {
        if (Replicas.Remove(nodeId, out var replica))
            await replica.DisposeAsync();
    }

    /// <summary>
    /// Restarts a crashed node from its simulated durable state.
    /// </summary>
    public Task RestartAsync(string nodeId, CancellationToken cancellationToken = default) {
        CreateReplica(nodeId);
        return Replicas[nodeId].StartAsync(cancellationToken);
    }

    /// <summary>
    /// Advances virtual time by <paramref name="virtualTime"/> — firing any due election/heartbeat
    /// timers and any in-flight simulated network deliveries — then waits for every currently-running
    /// replica's actor loop to finish processing whatever that firing produced.
    /// </summary>
    public async Task StepAsync(TimeSpan virtualTime, CancellationToken cancellationToken = default) {
        _timeProvider.Advance(virtualTime);
        await Task.WhenAll(Replicas.Values.Select(replica => replica.DrainAsync(cancellationToken)));
    }

    /// <summary>
    /// Simulates one RPC hop-and-back from <paramref name="fromNodeId"/> to
    /// <paramref name="toNodeId"/>: a partition check and a randomised delay/drop for the
    /// request, the actual in-process call once it "arrives", then the same for the response.
    /// </summary>
    internal async Task<TResponse> DeliverAsync<TRequest, TResponse>(
        string fromNodeId, string toNodeId, TRequest request,
        Func<RaftReplica, TRequest, CancellationToken, ValueTask<TResponse>> receive,
        CancellationToken cancellationToken) {
        await SimulateHopAsync(fromNodeId, toNodeId, cancellationToken);

        if (!Replicas.TryGetValue(toNodeId, out var target))
            throw new InvalidOperationException($"'{toNodeId}' is not currently running (crashed).");

        var response = await receive(target, request, cancellationToken);

        await SimulateHopAsync(toNodeId, fromNodeId, cancellationToken);
        return response;
    }

    async Task SimulateHopAsync(string fromNodeId, string toNodeId, CancellationToken cancellationToken) {
        if (!CanReach(fromNodeId, toNodeId))
            throw new InvalidOperationException($"'{fromNodeId}' cannot currently reach '{toNodeId}' (partitioned).");

        var (drop, delay) = SampleHop();
        delay = _options.AllowReorder ? delay : ClampToArrivalOrder(fromNodeId, toNodeId, delay);

        await Task.Delay(delay, _timeProvider, cancellationToken);

        // Re-checked after the delay: the partition may have changed while this hop was "in flight".
        if (drop || !CanReach(fromNodeId, toNodeId))
            throw new TimeoutException($"Simulated network dropped a message from '{fromNodeId}' to '{toNodeId}'.");
    }

    (bool Drop, TimeSpan Delay) SampleHop() {
        lock (_randomLock) {
            var drop = _random.NextDouble() < _options.MessageDropProbability;
            var minTicks = _options.MinDelay.Ticks;
            var maxTicks = _options.MaxDelay.Ticks;
            var delay = minTicks >= maxTicks ? _options.MinDelay : TimeSpan.FromTicks(_random.NextInt64(minTicks, maxTicks));
            return (drop, delay);
        }
    }

    // AllowReorder = false: force delivery between this pair of nodes to stay in send order,
    // regardless of the independently sampled delay — a later send never arrives before an
    // earlier one already scheduled to this same (from, to) pair.
    TimeSpan ClampToArrivalOrder(string fromNodeId, string toNodeId, TimeSpan sampledDelay) {
        lock (_arrivalLock) {
            var now = _timeProvider.GetUtcNow();
            var earliestArrival = now + sampledDelay;
            var key = (fromNodeId, toNodeId);

            if (_lastScheduledArrival.TryGetValue(key, out var previousArrival) && previousArrival >= earliestArrival)
                earliestArrival = previousArrival + TimeSpan.FromTicks(1);

            _lastScheduledArrival[key] = earliestArrival;
            return earliestArrival - now;
        }
    }

    bool CanReach(string fromNodeId, string toNodeId) =>
        fromNodeId == toNodeId || _islands.Any(island => island.Contains(fromNodeId) && island.Contains(toNodeId));

    public async ValueTask DisposeAsync() {
        foreach (var replica in Replicas.Values)
            await replica.DisposeAsync();
    }
}
