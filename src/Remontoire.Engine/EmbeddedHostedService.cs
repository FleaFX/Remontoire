using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

/// <summary>
/// Runs the embedded engine: the meta-group and one group per stream, each a Raft group without
/// peers under <see cref="RemontoireEngineOptions.DataDirectory"/>. A group opens as soon as the
/// meta log assigns a stream to it — while the log replays at startup, and whenever a stream is
/// created later, in-process or over the admin endpoint. Startup completes once every group has
/// caught up, so nothing is served from stale state.
/// </summary>
sealed class EmbeddedHostedService(
    RemontoireEngineOptions options,
    RemontoireNode node,
    RaftReplicaRegistry registry,
    ShardAssignmentTable assignmentTable,
    MetaLogJournal metaLogJournal,
    ILoggerFactory loggerFactory
) : IHostedService {
    /// <summary>
    /// The node id of every embedded group; there is only one node.
    /// </summary>
    internal const string NodeId = "embedded";

    // Without a client address no remote client can reach this node; this one never resolves (RFC 2606).
    static readonly Uri NoClientAddress = new("http://embedded.invalid");
    static readonly TimeSpan StartTimeout = TimeSpan.FromSeconds(30);

    readonly CancellationTokenSource _cts = new();
    Task? _groupOpener;

    /// <summary>
    /// The only member of every embedded group: this node, at its client address.
    /// </summary>
    /// <param name="options">The engine options.</param>
    /// <returns>The member to register.</returns>
    internal static ShardGroupMember Member(RemontoireEngineOptions options) => new(NodeId, options.ClientAddress ?? NoClientAddress);

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken) {
        node.RetentionTickInterval = options.RetentionTickInterval;
        await node.OpenMetaGroupAsync(SingleNodeConfig(RemontoireNode.MetaGroupId), Path.Combine(options.DataDirectory, "meta"),
            NoPeersRaftTransport.Instance, cancellationToken);
        _groupOpener = Task.Run(() => OpenGroupsAsync(_cts.Token));

        if (!await ConditionPoller.WaitUntilAsync(() => node.IsMetaCaughtUp && AllAssignedGroupsCaughtUp(), StartTimeout,
                cancellationToken: cancellationToken))
            throw new TimeoutException($"Remontoire did not catch up within {StartTimeout.TotalSeconds} seconds; see the log.");

        await RegisterClientAddressAsync(cancellationToken);
        node.MarkStarted();
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken) {
        await _cts.CancelAsync();
        if (_groupOpener is not null)
            await _groupOpener;
        await node.StopAsync();
        _cts.Dispose();
    }

    // The journal's only reader here, so groups open one at a time and never twice.
    async Task OpenGroupsAsync(CancellationToken cancellationToken) {
        var logger = loggerFactory.CreateLogger<EmbeddedHostedService>();
        try {
            await foreach (var (_, payload, _, _) in metaLogJournal.WatchAsync(0, cancellationToken)) {
                if (MetaLogRecord.Decode(payload) is not Cutover cutover || node.HostsGroup(cutover.ToGroupId))
                    continue;

                // A group id names a directory: one that came in over the admin endpoint and isn't a
                // safe name is never opened.
                if (!EmbeddedRemontoire.IsValidName(cutover.ToGroupId)) {
                    logger.LogError("Group {GroupId} of stream {StreamName} is not a valid directory name; it is not opened.",
                        cutover.ToGroupId, cutover.StreamName);
                    continue;
                }

                try {
                    await node.OpenGroupAsync(SingleNodeConfig(cutover.ToGroupId), Path.Combine(options.DataDirectory, "groups", cutover.ToGroupId),
                        NoPeersRaftTransport.Instance, cancellationToken);
                } catch (Exception ex) when (ex is not OperationCanceledException) {
                    logger.LogError(ex, "Could not open group {GroupId} of stream {StreamName}.", cutover.ToGroupId, cutover.StreamName);
                }
            }
        } catch (OperationCanceledException) {
            // Expected shutdown path — StopAsync cancels and awaits this.
        }
    }

    bool AllAssignedGroupsCaughtUp() =>
        assignmentTable.EnumerateAssignments().All(assignment => node.IsCaughtUp(assignment.GroupId));

    // The address is no part of a stream's identity: registering the current one at every start lets
    // it change between starts, and lets a stream created without one be reached once it is set.
    async Task RegisterClientAddressAsync(CancellationToken cancellationToken) {
        if (!registry.TryGet(RemontoireNode.MetaGroupId, out var metaReplica))
            return;

        var member = Member(options);
        foreach (var groupId in assignmentTable.EnumerateAssignments().Select(assignment => assignment.GroupId).Distinct()) {
            if (assignmentTable.TryGetGroup(groupId, out var group) && group.Members is [var only] && only == member)
                continue;

            await metaReplica.ProposeAsync(
                new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new RegisterGroup(groupId, [member]))), cancellationToken);
        }
    }

    // No peers, so no split votes: a short election timeout only shortens the wait for leadership,
    // at startup and when a stream is created.
    static RaftReplicaConfig SingleNodeConfig(string groupId) => new(
        GroupId: groupId,
        NodeId: NodeId,
        Peers: [],
        HeartbeatInterval: TimeSpan.FromMilliseconds(50),
        ElectionTimeoutMin: TimeSpan.FromMilliseconds(10),
        ElectionTimeoutMax: TimeSpan.FromMilliseconds(20));
}
