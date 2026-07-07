using System.Buffers;

namespace Remontoire.Storage;

/// <summary>
/// A <see cref="LogEntry"/> handed to a caller of <see cref="ShardLog"/>. Always dispose —
/// whether the entry came from a still-pooled SST read (real work happens) or from the
/// MemTable (nothing to release; the MemTable owns that memory for its own lifetime, so
/// disposing here is a correct no-op, not a stub) is an implementation detail the caller
/// does not need to know or branch on.
/// </summary>
public readonly struct LogEntryHandle(LogEntry entry, IMemoryOwner<byte>? owner = null) : IDisposable {
    /// <summary>
    /// The log entry this handle wraps.
    /// </summary>
    public LogEntry Entry { get; } = entry;

    /// <inheritdoc />
    public void Dispose() => owner?.Dispose();
}
