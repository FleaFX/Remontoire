namespace Remontoire.Storage;

/// <summary>
/// Distinguishes the purpose of a <see cref="WalRecord"/>.
/// </summary>
public enum WalRecordType : byte {
    /// <summary>
    /// A new message appended to the shard's log.
    /// </summary>
    Append = 0,

    /// <summary>
    /// A consumer-group acknowledgment of one or more previously appended messages.
    /// </summary>
    Ack = 1,

    /// <summary>
    /// A change to the shard's Raft group membership.
    /// </summary>
    ShardConfigChange = 2,

    /// <summary>
    /// An empty entry used by Raft to establish leader authority in a new term.
    /// </summary>
    NoOp = 3,
}
