using System.IO.Hashing;

namespace Remontoire.Sharding;

/// <summary>
/// Deterministically maps a partition key to a virtual shard index, per the routing rule
/// specified in the product design document (§4.4): <c>hash(key) mod virtualShardCount</c>.
/// This mapping never changes for a given (key, virtualShardCount, algorithm) triple — it is
/// the sole implementation, shared byte-for-byte between <c>Remontoire.Client</c> and
/// <c>Remontoire.Server</c>, of the single most safety-critical computation in the routing
/// path: any divergence between the two sides silently misroutes messages.
/// </summary>
public static class ShardRouter {
    const long Seed = 0;

    /// <summary>
    /// Computes the virtual shard index for <paramref name="partitionKey"/> within a stream
    /// that has <paramref name="virtualShardCount"/> virtual shards, using
    /// <paramref name="algorithm"/>. Pure and allocation-free — safe to call on every publish,
    /// on both the client and server side. The default value of <paramref name="algorithm"/>
    /// must never change once a second member of <see cref="RoutingAlgorithm"/> exists — a
    /// caller relying on the default is a caller that has not been updated to read a stream's
    /// actual, stored algorithm choice, and must keep getting today's behavior, not silently
    /// shift to a new one.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="virtualShardCount"/> is not positive, or <paramref name="algorithm"/> is not a known member.
    /// </exception>
    public static int GetVirtualShardIndex(ReadOnlySpan<byte> partitionKey, int virtualShardCount, RoutingAlgorithm algorithm = RoutingAlgorithm.XxHash3V1) {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualShardCount);

        var hash = algorithm switch {
            RoutingAlgorithm.XxHash3V1 => XxHash3.HashToUInt64(partitionKey, Seed),
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unknown routing algorithm.")
        };

        return (int)(hash % (ulong)virtualShardCount);
    }
}
