namespace Remontoire.Storage.Serialization;

/// <summary>
/// The outcome of attempting to read one <see cref="WalRecord"/> from a byte buffer.
/// </summary>
enum WalRecordReadStatus {
    /// <summary>
    /// A complete, valid record was read.
    /// </summary>
    Success,

    /// <summary>
    /// The buffer does not yet contain enough bytes for a full record; more data is needed
    /// (e.g. from disk).
    /// </summary>
    Incomplete,

    /// <summary>
    /// Enough bytes were present, but the length or checksum did not validate — a torn write
    /// or corruption.
    /// </summary>
    Corrupt,
}
