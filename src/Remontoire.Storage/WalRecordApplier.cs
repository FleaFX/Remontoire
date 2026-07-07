namespace Remontoire.Storage;

/// <summary>
/// The only place a <see cref="WalRecord"/> is translated into materialized-state (MemTable)
/// updates — used identically by recovery and by live-apply so the two can never diverge.
/// </summary>
sealed class WalRecordApplier(MemTable target) {
    /// <summary>
    /// Applies <paramref name="record"/> to the target MemTable. <see cref="WalRecordType.Append"/>
    /// builds a <see cref="LogEntry"/> and appends it; every other record type is a phase-1
    /// no-op (reserved for Remontoire.Messaging / Remontoire.Raft in later phases).
    /// </summary>
    public void Apply(WalRecord record) {
        if (record.RecordType != WalRecordType.Append)
            return;

        target.Append(new LogEntry(record.LogicalOffset, record.TimestampMicros, record.PartitionKey, record.Headers, record.Payload));
    }
}
