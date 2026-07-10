using System.Collections.Concurrent;

namespace Remontoire.Server;

/// <summary>
/// Tracks which physical groups are currently pausing admission for an in-progress reshard's
/// local pause step — a short, deliberately NOT Raft-committed signal local to this node, checked
/// by <see cref="Grpc.RemontoireClientGrpcService"/> before dispatching a call to a paused group.
/// A brand new leader (elected after a crash during the pause) starts with nothing paused, exactly
/// matching the design's own crash-safety analysis: a paused group that loses its leader simply
/// resumes normal writes, no cutover having ever committed.
/// </summary>
public sealed class MigrationAdmissionGate {
    readonly ConcurrentDictionary<string, byte> _pausedGroups = new();

    /// <summary>
    /// Pauses admission for <paramref name="groupId"/> until <see cref="Resume"/> is called.
    /// </summary>
    public void Pause(string groupId) => _pausedGroups[groupId] = 0;

    /// <summary>
    /// Resumes admission for <paramref name="groupId"/>.
    /// </summary>
    public void Resume(string groupId) => _pausedGroups.TryRemove(groupId, out _);

    /// <summary>
    /// Whether <paramref name="groupId"/> is currently pausing admission.
    /// </summary>
    public bool IsPaused(string groupId) => _pausedGroups.ContainsKey(groupId);
}
