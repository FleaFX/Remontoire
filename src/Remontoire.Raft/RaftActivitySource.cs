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
}
