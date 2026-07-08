<img src="assets/logo.svg" alt="Remontoire" width="480">

Remontoire is a durable, Raft-replicated, sharded messaging log for inbox/outbox patterns — self-hosted, at-least-once delivery, with either ack-driven retention (queue-style) or unbounded retention (event-sourcing-style).

> **Status:** active development.

## Core ideas

- **Durable log per shard.** Every shard is a write-ahead-logged, LSM-backed append-only log, replicated via a Raft group.
- **One mechanism, two uses.** The same stream primitive serves as an inbox (external arrivals) or an outbox (outgoing dispatch) — the difference is only in how it's used, not in the underlying storage or replication.
- **Sharding for throughput, reshardable by design.** A stream's routing is fixed at creation, but how many physical Raft groups serve it can grow later without breaking that routing.
- **Two retention models.** Ack-driven (consumer-groups track per-message acknowledgment; pruning happens once all groups have acked and an audit period has elapsed) or unbounded (time/size-based retention, for replay).
- **Consensus is the unit of durability.** A write is only considered safe once a majority of a shard's nodes have replicated and persisted it — never on a single node's local fsync alone.

## Relation to MSSP

Remontoire shares conceptual ground with [MSSP](https://github.com/FleaFX/mssp) (WAL, LSM-tree storage, Raft consensus) — lessons learned there inform this design. It's a clean, independent implementation; no code is shared or reused between the two projects.

The two aren't competing designs — they solve different problems. MSSP is an aggregate-of-record event store: many individually addressable streams (one per aggregate), each with its own optimistic-concurrency check, plus a global commit order for projections. Remontoire is a distribution layer: few, coarser streams built for high-throughput, ack-driven or replayable message delivery, where messages from many keys are interleaved within a shard rather than individually addressable. Remontoire is a good fit for carrying already-committed events *out* of a system like MSSP to downstream consumers — not a replacement for the aggregate store itself.

## Building

Requires .NET 10 SDK.

```bash
dotnet build src/Remontoire.slnx
dotnet test src/Remontoire.slnx
```
