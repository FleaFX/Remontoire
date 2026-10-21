namespace Remontoire.Sharding;

/// <summary>
/// Identifies which routing computation a stream uses. A stream's value never changes after
/// creation — this is a per-stream, immutable choice, the same category of setting as a
/// stream's virtual shard count, not a per-call dial. New members are additive only; existing
/// members' numeric values and behavior are frozen forever once shipped.
/// </summary>
public enum RoutingAlgorithm : byte {
    /// <summary>
    /// <see cref="System.IO.Hashing.XxHash3"/> with a fixed, zero seed. See <see cref="ShardRouter"/>.
    /// </summary>
    XxHash3V1 = 0,
}
