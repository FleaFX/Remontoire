namespace Remontoire.Sharding;

/// <summary>
/// The wire tag identifying which <see cref="MetaLogRecord"/> case an encoded record holds.
/// </summary>
public enum MetaLogRecordType : byte {
    /// <summary>
    /// Tag for <see cref="CreateStream"/>.
    /// </summary>
    CreateStream = 0,

    /// <summary>
    /// Tag for <see cref="RegisterGroup"/>.
    /// </summary>
    RegisterGroup = 1,

    /// <summary>
    /// Tag for <see cref="MigrationStarted"/>.
    /// </summary>
    MigrationStarted = 2,

    /// <summary>
    /// Tag for <see cref="MigrationAborted"/>.
    /// </summary>
    MigrationAborted = 3,

    /// <summary>
    /// Tag for <see cref="Cutover"/>.
    /// </summary>
    Cutover = 4,

    /// <summary>
    /// Tag for <see cref="MigrationCompleted"/>.
    /// </summary>
    MigrationCompleted = 5,
}
