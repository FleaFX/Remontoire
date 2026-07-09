using System.Buffers;

namespace Remontoire.Storage.Serialization;

/// <summary>
/// The result of <see cref="WalRecordSerializer.TryRead"/>. Disposing releases the pooled
/// buffer backing <see cref="Record"/>'s variable-length fields, if any — safe to call
/// regardless of <see cref="Status"/>, including on a default/never-assigned instance.
/// </summary>
public readonly struct WalReadResult(WalRecordReadStatus status, WalRecord record, int bytesConsumed, IMemoryOwner<byte>? owner) : IDisposable {
    /// <summary>
    /// Whether a record was read, and if not, why.
    /// </summary>
    public WalRecordReadStatus Status { get; } = status;

    /// <summary>
    /// The record that was read; default when <see cref="Status"/> is not
    /// <see cref="WalRecordReadStatus.Success"/>.
    /// </summary>
    public WalRecord Record { get; } = record;

    /// <summary>
    /// The number of bytes consumed from the input; zero when <see cref="Status"/> is not
    /// <see cref="WalRecordReadStatus.Success"/>.
    /// </summary>
    public int BytesConsumed { get; } = bytesConsumed;

    /// <inheritdoc />
    public void Dispose() => owner?.Dispose();
}
