namespace Remontoire.Sharding;

/// <summary>
/// A stream's own, immutable sharding choices — fixed once at stream creation and never
/// changed afterward. Consulted by both client and server code before calling
/// <see cref="ShardRouter.GetVirtualShardIndex"/> — never invented locally, never guessed.
/// </summary>
/// <param name="StreamName">The stream this configuration belongs to.</param>
/// <param name="VirtualShardCount">
/// Fixed at creation. Passed as-is to <see cref="ShardRouter.GetVirtualShardIndex"/>'s
/// <c>virtualShardCount</c> parameter.
/// </param>
/// <param name="RoutingAlgorithm">
/// Fixed at creation. Never falls back to the parameter default once this type exists — a
/// caller with a <see cref="StreamShardingConfig"/> in hand always passes this field explicitly.
/// </param>
public readonly record struct StreamShardingConfig(string StreamName, int VirtualShardCount, RoutingAlgorithm RoutingAlgorithm);
