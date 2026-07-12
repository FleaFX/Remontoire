namespace Remontoire.Tests;

// xUnit runs test classes in parallel by default, with a degree of parallelism tied to the
// machine's core count. Every class in this collection spins up multiple real Kestrel hosts, real
// TCP sockets, and real Raft heartbeat/election timers (tens of milliseconds) — running several of
// them fully concurrently starves them all of CPU, making otherwise-generous timeouts fire late.
// The resulting failures land on whichever test happened to be running at that moment, not
// consistently on any one test — the signature of resource contention, not a logic bug. Grouping
// them into one collection with parallelization disabled makes them run sequentially relative to
// each other, while every other (fast, unit-level) test class keeps running in parallel as before.
[CollectionDefinition("RealNetwork", DisableParallelization = true)]
public sealed class RealNetworkCollection;
