namespace Remontoire.Raft;

/// <summary>
/// Per-run knobs for a <see cref="SimulatedCluster"/>, all driven by the cluster's shared seed.
/// </summary>
/// <param name="MessageDropProbability">Chance, per hop (send or return), that a message is lost.</param>
/// <param name="MinDelay">Lower bound of the randomised, per-hop simulated network delay.</param>
/// <param name="MaxDelay">Upper bound of the randomised, per-hop simulated network delay.</param>
/// <param name="AllowReorder">
/// When <see langword="true"/>, independent random delays are free to reorder messages between
/// the same two nodes. When <see langword="false"/>, delivery between any two nodes is forced
/// monotonic (first sent, first delivered) regardless of sampled delay.
/// </param>
/// <param name="SnapshotThresholdEntries">
/// When set, every replica is wired with a trivial (no real segments — <see cref="InMemoryRaftLog"/>
/// has none) <c>prepareSnapshot</c>/<c>installSnapshot</c> pair and this threshold, so
/// self-triggered snapshotting and <c>InstallSnapshot</c> catch-up actually run in the
/// simulation. <see langword="null"/> (default) leaves snapshotting off, matching every
/// scenario that predates this option.
/// </param>
sealed record SimulationOptions(
    double MessageDropProbability,
    TimeSpan MinDelay,
    TimeSpan MaxDelay,
    bool AllowReorder,
    ulong? SnapshotThresholdEntries = null);
