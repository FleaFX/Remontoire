namespace Remontoire.Sharding;

/// <summary>
/// Identifies one reshard operation end-to-end — generated once by the operator at the
/// migration-started step and carried by every subsequent command for the same operation
/// (migration-started, migration-aborted, cutover, migration-completed). A command whose id
/// doesn't match the migration currently in progress for a given (stream, virtual shard) pair is
/// rejected — the same "one at a time" discipline membership changes already apply, here applied
/// to reshard operations instead.
/// </summary>
public readonly record struct MigrationId(Guid Value);
