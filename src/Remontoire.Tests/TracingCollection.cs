namespace Remontoire.Tests;

// An ActivityListener is process-wide: once attached, it hears every span of every concurrently
// running test. The tracing tests collect the spans of their own embedded host only, and their
// last test depends on nobody else listening to "Remontoire" at that moment, so they run
// sequentially relative to each other rather than in parallel.
[CollectionDefinition("Tracing", DisableParallelization = true)]
public sealed class TracingCollection;
