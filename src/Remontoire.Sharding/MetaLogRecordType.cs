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

    /// <summary>
    /// Tag for <see cref="SetConsumerGroupAckMode"/>.
    /// </summary>
    SetConsumerGroupAckMode = 6,

    /// <summary>
    /// Tag for <see cref="SetConsumerGroupMandatory"/>.
    /// </summary>
    SetConsumerGroupMandatory = 7,

    /// <summary>
    /// Tag for <see cref="SetStreamRetentionPolicy"/>.
    /// </summary>
    SetStreamRetentionPolicy = 8,

    /// <summary>
    /// Tag for <see cref="SetStreamCheckpointInterval"/>.
    /// </summary>
    SetStreamCheckpointInterval = 9,

    /// <summary>
    /// Tag for <see cref="SetProduceAcl"/>.
    /// </summary>
    SetProduceAcl = 10,

    /// <summary>
    /// Tag for <see cref="SetConsumeAcl"/>.
    /// </summary>
    SetConsumeAcl = 11,

    /// <summary>
    /// Tag for <see cref="SetStreamSubjectClaimType"/>.
    /// </summary>
    SetStreamSubjectClaimType = 12,
}
