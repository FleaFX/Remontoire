namespace Remontoire.Tests;

// Any test that redirects the process-wide static Console.Out (to capture JSON console log
// output) must run serialized relative to every other such test — two of them running
// concurrently could have one test's log lines land in the other's captured buffer, or one
// test's own cleanup restore the real console mid-flight of another. Same rationale as
// RealNetworkCollection, applied to this different shared static.
[CollectionDefinition("ConsoleOutput", DisableParallelization = true)]
public sealed class ConsoleOutputCollection;
