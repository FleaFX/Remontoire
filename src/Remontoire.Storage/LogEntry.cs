namespace Remontoire.Storage;

/// <summary>
/// A single entry in a shard's materialized state (<see cref="MemTable"/>/SST) — the
/// complement of <see cref="WalRecord"/> minus its log-replication metadata
/// (<c>RecordType</c>/<c>RaftTerm</c>/<c>RaftIndex</c>), which this layer never needs.
/// </summary>
/// <param name="LogicalOffset">The consumer-visible, monotonically increasing, gapless offset within the shard.</param>
/// <param name="TimestampMicros">The ingest timestamp, in microseconds since the Unix epoch, assigned by the leader.</param>
/// <param name="PartitionKey">The raw, UTF-8-encoded partition key.</param>
/// <param name="Headers">Free-form key/value metadata attached to the message.</param>
/// <param name="Payload">The opaque message payload.</param>
public readonly record struct LogEntry(
    ulong LogicalOffset,
    ulong TimestampMicros,
    ReadOnlyMemory<byte> PartitionKey,
    IReadOnlyList<WalHeader> Headers,
    ReadOnlyMemory<byte> Payload
);
