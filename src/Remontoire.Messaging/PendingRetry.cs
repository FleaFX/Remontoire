namespace Remontoire.Messaging;

/// <summary>
/// A nacked message a consumer group is to be offered again, before any later message.
/// </summary>
/// <param name="Offset">The message's offset.</param>
/// <param name="Attempt">The delivery attempt of the next offer: one more than the attempt that was nacked.</param>
/// <param name="NotBeforeMicros">From when on the message may be offered again, in microseconds since the Unix epoch, on the leader's clock.</param>
public sealed record PendingRetry(ulong Offset, int Attempt, ulong NotBeforeMicros);
