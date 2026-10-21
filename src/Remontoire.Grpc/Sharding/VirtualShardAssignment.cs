namespace Remontoire.Sharding;

/// <summary>
/// Which physical group currently serves one virtual shard of one stream, and — during an
/// in-progress reshard — which group it's migrating to. <see cref="MigratingToGroupId"/> and
/// <see cref="MigrationId"/> are non-null only between a migration-started and a cutover command;
/// routing itself (which group a publish/consume actually goes to) always follows
/// <see cref="GroupId"/>, never <see cref="MigratingToGroupId"/>, until the cutover command flips
/// <see cref="GroupId"/> itself — the migration fields exist purely so a client/server can show
/// operators progress, so <see cref="MigratingToGroupId"/> becomes the eventual bulk-copy target,
/// and so a later command can be checked against <see cref="MigrationId"/> before being allowed to
/// change anything — not so routing gradually shifts.
/// </summary>
public readonly record struct VirtualShardAssignment(
    string StreamName, int VirtualShardIndex, string GroupId, string? MigratingToGroupId = null, MigrationId? MigrationId = null);
