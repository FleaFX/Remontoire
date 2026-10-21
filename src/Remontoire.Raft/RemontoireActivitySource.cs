using System.Diagnostics;

namespace Remontoire.Raft;

/// <summary>
/// The one <see cref="ActivitySource"/> of Remontoire, named <see cref="Name"/>: the spans
/// <c>publish</c>, <c>receive</c>, <c>wal-append</c> and <c>raft-replicate</c>. Pure BCL, no
/// observability-library reference. A span is only created once the host listens to this source
/// (with OpenTelemetry: <c>AddSource("Remontoire")</c>); otherwise starting one is a cheap no-op.
/// </summary>
public static class RemontoireActivitySource {
    /// <summary>
    /// The source's name, for <c>AddSource</c>.
    /// </summary>
    public const string Name = "Remontoire";

    /// <summary>
    /// The source every Remontoire span is started from.
    /// </summary>
    public static readonly ActivitySource Source = new(Name);

    /// <summary>
    /// Starts <paramref name="name"/> with <paramref name="parentContext"/> as its explicit parent
    /// when one is given. When it isn't (a background-driven propose with no real caller
    /// <c>Activity</c>, or a receive that must start a trace of its own), this produces a rootless
    /// span, never one that silently inherits whatever happens to be ambient: passing
    /// <see langword="default"/>(<see cref="ActivityContext"/>) to <c>StartActivity</c> does NOT mean
    /// "no parent" — the API treats it as "not specified" and falls back to <see cref="Activity.Current"/>.
    /// Suppressing <see cref="Activity.Current"/> for the duration of the call (and restoring it
    /// right after) is the only way to force a genuinely parentless activity here; the started
    /// activity is therefore not made current.
    /// </summary>
    /// <param name="name">The span's name.</param>
    /// <param name="parentContext">The explicit parent, or <see langword="null"/> for none.</param>
    /// <param name="kind">The span's kind.</param>
    /// <param name="links">The span's links, if any.</param>
    /// <returns>The started span, or <see langword="null"/> when nobody listens.</returns>
    public static Activity? StartActivity(
        string name,
        ActivityContext? parentContext,
        ActivityKind kind = ActivityKind.Internal,
        IEnumerable<ActivityLink>? links = null
    ) {
        if (parentContext is { } context)
            return Source.StartActivity(name, kind, context, links: links);

        var ambient = Activity.Current;
        Activity.Current = null;
        try {
            return Source.StartActivity(name, kind, parentContext: default, links: links);
        } finally {
            Activity.Current = ambient;
        }
    }
}
