namespace Remontoire.Sharding;

/// <summary>
/// Which physical group currently serves one virtual shard of one stream, and — during an
/// in-progress reshard — which group it's migrating to. <see cref="MigratingToGroupId"/> is
/// non-null only between a migration-started and a cutover command; routing itself (which
/// group a publish/consume actually goes to) always follows <see cref="GroupId"/>, never
/// <see cref="MigratingToGroupId"/>, until the cutover command flips <see cref="GroupId"/>
/// itself — the migration field exists purely so a client/server can show operators progress
/// and so <see cref="MigratingToGroupId"/> becomes the eventual bulk-copy target, not so
/// routing gradually shifts.
/// </summary>
public readonly record struct VirtualShardAssignment(string StreamName, int VirtualShardIndex, string GroupId, string? MigratingToGroupId = null);
