using System.Security.Cryptography;
using System.Text;
using Remontoire.Sharding;

namespace Remontoire.Engine;

/// <summary>
/// The meta-log records that create a stream — shared by the admin endpoint and the embedded
/// engine, so a stream is created the same way whichever path proposes it.
/// </summary>
public static class StreamBootstrap {
    /// <summary>
    /// The records that create <paramref name="streamName"/>: the stream, its groups, and per virtual
    /// shard a migration onto its initial group that cuts over at once.
    /// </summary>
    /// <param name="streamName">The stream to create.</param>
    /// <param name="virtualShardCount">The number of virtual shards.</param>
    /// <param name="routingAlgorithm">How a partition key maps to a virtual shard.</param>
    /// <param name="groups">The groups the stream's virtual shards start on.</param>
    /// <param name="assignments">Per virtual shard, the group it starts on.</param>
    /// <returns>The records, in the order they must be proposed.</returns>
    public static IEnumerable<MetaLogRecord> Records(
        string streamName,
        int virtualShardCount,
        RoutingAlgorithm routingAlgorithm,
        IEnumerable<RegisterGroup> groups,
        IEnumerable<(int VirtualShardIndex, string GroupId)> assignments
    ) {
        yield return new CreateStream(streamName, virtualShardCount, routingAlgorithm);
        foreach (var group in groups)
            yield return group;

        foreach (var (virtualShardIndex, groupId) in assignments) {
            var migrationId = DeriveBootstrapMigrationId(streamName, virtualShardIndex);
            yield return new MigrationStarted(migrationId, streamName, virtualShardIndex, groupId, groupId);
            yield return new Cutover(migrationId, streamName, virtualShardIndex, groupId);
        }
    }

    // Deterministic, not Guid.NewGuid(): a retried CreateStream call must reproduce the exact same
    // MigrationId per (stream, virtual shard) so ShardAssignmentTable.Apply's own "same migration
    // already started" / "same migration already cut over" idempotency recognizes a retry as a
    // no-op instead of rejecting it as a conflicting, second migration for the same shard. Not RFC
    // 4122 section 4.3 name-based UUID version 5 — this only needs determinism and collision
    // resistance, not that scheme's specific version/variant bits, so a plain SHA-256-derived Guid
    // is simpler and needs no extra library.
    internal static MigrationId DeriveBootstrapMigrationId(string streamName, int virtualShardIndex) {
        var seed = $"{streamName}:{virtualShardIndex}:bootstrap";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(seed));
        return new MigrationId(new Guid(hash.AsSpan(0, 16)));
    }
}
