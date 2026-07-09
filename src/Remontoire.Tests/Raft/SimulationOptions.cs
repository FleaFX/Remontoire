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
sealed record SimulationOptions(
    double MessageDropProbability,
    TimeSpan MinDelay,
    TimeSpan MaxDelay,
    bool AllowReorder);
