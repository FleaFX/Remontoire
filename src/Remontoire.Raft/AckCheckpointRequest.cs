namespace Remontoire.Raft;

/// <summary>
/// What one periodic ack checkpoint proposes for replication — a single watermark, not a list of
/// offsets (<see cref="AckRequest.Offsets"/>'s shape): checkpoint mode only ever replicates the
/// contiguous low watermark, never the out-of-order selective-ack set. Same
/// non-consuming-a-<see cref="ProposeResult.LogicalOffset"/> contract as <see cref="AckRequest"/>.
/// </summary>
public readonly record struct AckCheckpointRequest(string ConsumerGroup, ulong Watermark);
