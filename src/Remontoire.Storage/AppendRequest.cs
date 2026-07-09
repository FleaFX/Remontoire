namespace Remontoire.Storage;

/// <summary>
/// What a producer offers when proposing a new message — deliberately narrower than
/// <see cref="WalRecord"/> (no <c>RecordType</c>/<c>RaftTerm</c>/<c>RaftIndex</c>, which are
/// assigned elsewhere) and independent of any WAL on-disk format.
/// </summary>
public readonly record struct AppendRequest(
    ReadOnlyMemory<byte> PartitionKey,
    IReadOnlyList<Header> Headers,
    ReadOnlyMemory<byte> Payload
);
