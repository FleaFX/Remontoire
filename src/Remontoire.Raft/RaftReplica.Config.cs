namespace Remontoire.Raft;

/// <summary>
/// Immutable runtime configuration for one <see cref="RaftReplica"/>. This same object is what
/// the transport is constructed from — deliberately, so the RPC timeout can never silently
/// fail to reach the transport (there is no second place to forget it).
/// </summary>
/// <param name="GroupId">The physical shard group this replica belongs to.</param>
/// <param name="NodeId">This node's unique identifier within the group.</param>
/// <param name="Peers">All other members of the group; empty for a single-node (test) group.</param>
/// <param name="HeartbeatInterval">Leader heartbeat cadence. Starting point: 50 ms.</param>
/// <param name="ElectionTimeoutMin">Lower bound of the randomized election timeout. Starting point: 250 ms.</param>
/// <param name="ElectionTimeoutMax">Upper bound of the randomized election timeout. Starting point: 500 ms.</param>
/// <param name="RpcTimeout">
/// Deadline applied to every outbound Raft RPC. Defaults to five times
/// <paramref name="HeartbeatInterval"/> when not set explicitly.
/// </param>
/// <param name="SnapshotChunkSizeBytes">Maximum bytes per <c>InstallSnapshot</c> chunk. Default 1 MiB.</param>
/// <param name="SnapshotThresholdEntries">
/// A snapshot is taken once the log tail beyond <c>SnapshotIndex</c> grows past this many
/// entries. Entry count, not bytes — keeps <see cref="IRaftLog"/> free of any size-estimation
/// contract. Starting point: 10,000; ignored entirely when no snapshot-preparation delegate is
/// wired up.
/// </param>
/// <param name="ElectionRandomSeed">Optional seed for the election-timeout randomization</param>
/// <param name="SnapshotStagingDirectory">
/// Where an incoming <c>InstallSnapshot</c> transfer writes its files while still in progress.
/// Defaults to a <see cref="GroupId"/>-scoped subdirectory under the system temp path.
/// </param>
public sealed record RaftReplicaConfig(
    string GroupId,
    string NodeId,
    IReadOnlyList<RaftGroupMember> Peers,
    TimeSpan HeartbeatInterval,
    TimeSpan ElectionTimeoutMin,
    TimeSpan ElectionTimeoutMax,
    TimeSpan? RpcTimeout = null,
    int SnapshotChunkSizeBytes = 1 << 20,
    ulong SnapshotThresholdEntries = 10_000,
    int? ElectionRandomSeed = null,
    string? SnapshotStagingDirectory = null
) {
    /// <summary>Resolves <see cref="SnapshotStagingDirectory"/>'s default when left unset.</summary>
    public string ResolvedSnapshotStagingDirectory =>
        SnapshotStagingDirectory ?? Path.Combine(Path.GetTempPath(), "remontoire-snapshot-staging", GroupId);

    /// <summary>Resolves <see cref="RpcTimeout"/>'s default when left unset: five times <see cref="HeartbeatInterval"/>.</summary>
    public TimeSpan ResolvedRpcTimeout => RpcTimeout ?? HeartbeatInterval * 5;
}
