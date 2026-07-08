namespace Remontoire.Storage;

/// <summary>
/// What a caller offers to <see cref="ShardLog.AppendAsync"/> — deliberately narrower than
/// <see cref="WalRecord"/> (no <c>RecordType</c>/<c>RaftTerm</c>/<c>RaftIndex</c>, which
/// <see cref="ShardLog"/> assigns itself) and independent of any WAL on-disk format.
/// </summary>
public readonly record struct AppendRequest(
    ReadOnlyMemory<byte> PartitionKey,
    IReadOnlyList<Header> Headers,
    ReadOnlyMemory<byte> Payload
);
