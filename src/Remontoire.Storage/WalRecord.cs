namespace Remontoire.Storage;

/// <summary>
/// A single, physical entry in a shard's write-ahead log. Internal to the WAL/log layer —
/// never exposed across <c>Remontoire.Storage</c>'s public boundary; see <see cref="LogEntry"/>
/// for the corresponding materialized-state representation.
/// </summary>
/// <param name="RecordType">Distinguishes what kind of entry this is.</param>
/// <param name="RaftTerm">The Raft term in which this entry was proposed. Always zero until Raft replication is wired in.</param>
/// <param name="RaftIndex">The position of this entry in the shard's Raft log. Always zero until Raft replication is wired in.</param>
/// <param name="LogicalOffset">The consumer-visible, monotonically increasing offset within the shard; meaningless for non-<see cref="WalRecordType.Append"/> entries.</param>
/// <param name="TimestampMicros">The ingest timestamp, in microseconds since the Unix epoch, assigned by the leader.</param>
/// <param name="PartitionKey">The raw, UTF-8-encoded partition key; kept as bytes to avoid decoding on the hot path.</param>
/// <param name="Headers">Free-form key/value metadata attached to the message.</param>
/// <param name="Payload">The opaque message payload.</param>
readonly record struct WalRecord(
    WalRecordType RecordType,
    ulong RaftTerm,
    ulong RaftIndex,
    ulong LogicalOffset,
    ulong TimestampMicros,
    ReadOnlyMemory<byte> PartitionKey,
    IReadOnlyList<WalHeader> Headers,
    ReadOnlyMemory<byte> Payload);
