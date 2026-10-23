namespace Remontoire.Raft;

/// <summary>
/// What one consumer-group negative acknowledgment proposes for replication: the message at
/// <paramref name="Offset"/> failed at delivery attempt <paramref name="Attempt"/>. Either it is
/// offered again <paramref name="Delay"/> after the record's own timestamp, or, with
/// <paramref name="Park"/>, it is parked. The leader decides which, so every replica applies the
/// same outcome. Same non-consuming-a-<see cref="ProposeResult.LogicalOffset"/> contract as
/// <see cref="AckRequest"/>.
/// </summary>
/// <param name="ConsumerGroup">The consumer group.</param>
/// <param name="Offset">The nacked message's offset.</param>
/// <param name="Attempt">The delivery attempt that failed, 1 for the first.</param>
/// <param name="Delay">How long after the record's timestamp the message is offered again; ignored when parking.</param>
/// <param name="Park">Whether the message is parked instead of offered again.</param>
/// <param name="Reason">Why handling failed; empty when none was given.</param>
public readonly record struct NackRequest(string ConsumerGroup, ulong Offset, int Attempt, TimeSpan Delay, bool Park, string Reason);
