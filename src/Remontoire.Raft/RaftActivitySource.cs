using System.Diagnostics;

namespace Remontoire.Raft;

/// <summary>
/// The one <see cref="ActivitySource"/> for every span this project starts (<c>wal-append</c>,
/// <c>raft-replicate</c>) — pure BCL, no NuGet dependency, so this doesn't touch the "lower layers
/// stay free of an observability-library reference" discipline the rest of this codebase follows.
/// Only actually collected once <c>Remontoire.Server</c>'s OpenTelemetry SDK setup calls
/// <c>AddSource("Remontoire.Raft")</c> — started activities are cheap no-ops otherwise.
/// </summary>
static class RaftActivitySource {
    public static readonly ActivitySource Source = new("Remontoire.Raft");

    /// <summary>
    /// Starts <paramref name="name"/> with <paramref name="parentContext"/> as its explicit parent
    /// when one is given. When it isn't (a background-driven propose with no real caller
    /// <c>Activity</c> — <see cref="AckCheckpointer"/>/<see cref="RetentionEvaluator"/>'s own
    /// periodic ticks, for instance), this must produce a rootless span, never one that silently
    /// inherits whatever happens to be ambient on the actor loop's own thread: passing
    /// <see langword="default"/>(<see cref="ActivityContext"/>) to <c>StartActivity</c>'s
    /// three-argument overload does NOT mean "no parent" — the API treats it as "not specified" and
    /// falls back to <see cref="Activity.Current"/>, which could be a stale, unrelated activity
    /// left over from however this actor loop's own <c>Task.Run</c> happened to be started.
    /// Suppressing <see cref="Activity.Current"/> for the duration of the call (and restoring it
    /// right after) is the only way to force a genuinely parentless activity here.
    /// </summary>
    public static Activity? StartActivity(string name, ActivityContext? parentContext) {
        if (parentContext is { } context)
            return Source.StartActivity(name, ActivityKind.Internal, context);

        var ambient = Activity.Current;
        Activity.Current = null;
        try {
            return Source.StartActivity(name, ActivityKind.Internal);
        } finally {
            Activity.Current = ambient;
        }
    }
}
