namespace Remontoire.Raft;

/// <summary>
/// What one consumer-group acknowledgment proposes for replication — one or more offsets, batched
/// in a single call. Unlike <see cref="Remontoire.Storage.AppendRequest"/>, this never consumes a
/// <see cref="ProposeResult.LogicalOffset"/>: an ack is not a consumer-visible message.
/// </summary>
public readonly record struct AckRequest(string ConsumerGroup, IReadOnlyList<ulong> Offsets);
