using System.Collections.Concurrent;

namespace Remontoire.Client;

/// <summary>
/// The last known leader address per group — shared across every call
/// (<see cref="IRemontoireProducer.PublishAsync"/>, <see cref="IRemontoireConsumer.AckAsync"/>,
/// <see cref="IRemontoireConsumer.ConsumeAsync"/>) on the same group, not a per-method local: a
/// successful call updates it for everyone's benefit, not just its own.
/// </summary>
sealed class LeaderAddressCache {
    readonly ConcurrentDictionary<string, Uri> _cache = new();

    /// <summary>
    /// The last known leader address for <paramref name="groupId"/>, or <see langword="null"/> if none is cached.
    /// </summary>
    public Uri? Get(string groupId) => _cache.GetValueOrDefault(groupId);

    /// <summary>
    /// Records <paramref name="address"/> as the current leader for <paramref name="groupId"/>.
    /// </summary>
    public void Update(string groupId, Uri address) => _cache[groupId] = address;

    /// <summary>
    /// Clears whatever is cached for <paramref name="groupId"/> — no hint is known until the next successful call.
    /// </summary>
    public void Invalidate(string groupId) => _cache.TryRemove(groupId, out _);
}
