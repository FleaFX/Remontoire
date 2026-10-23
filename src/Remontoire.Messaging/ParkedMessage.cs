namespace Remontoire.Messaging;

/// <summary>
/// A message a consumer group parked after its last retry: handled for the group's cursor, still
/// kept in the log until its maximum retention.
/// </summary>
/// <param name="Attempts">How often the message was offered before it was parked.</param>
/// <param name="Reason">The reason of the nack that parked it; empty when none was given.</param>
/// <param name="ParkedAtMicros">When it was parked, in microseconds since the Unix epoch, on the leader's clock.</param>
public sealed record ParkedMessage(int Attempts, string Reason, ulong ParkedAtMicros);
