using System.Collections.Immutable;

namespace Remontoire.Messaging;

/// <summary>
/// One consumer-group's acknowledgment progress on one shard: a low watermark (every offset
/// below this one is acked; zero means nothing has been acked yet — offsets are 0-based, so an
/// inclusive "last acked offset" cannot distinguish "offset 0 is acked" from "nothing is acked",
/// the same reason this codebase's other progress counters, e.g. a shard log's own
/// next-offset-to-apply, are exclusive rather than inclusive) plus a small set of out-of-order,
/// individually acked offsets above it — the TCP-selective-acknowledgment pattern.
/// </summary>
/// <remarks>
/// <see cref="Ack"/>/<see cref="ApplyLocally"/>/<see cref="AdvanceWatermarkTo"/> are <c>internal</c>
/// and mutate no field under a lock — <see cref="AckIndex"/>'s own actor mailbox is this type's
/// sole caller and sole writer, one message at a time, never concurrently with itself. The
/// selective-ack sets are immutable snapshots swapped via <see cref="Volatile"/>, so any other
/// thread's read (<see cref="IsAcked"/>, <see cref="LowWatermark"/>, <see cref="CommittedWatermark"/>)
/// sees one consistent, never-mutated-in-place snapshot without ever needing to go through the
/// mailbox itself — the same atomic-reference-swap discipline <c>ShardLog</c> already uses for its
/// own segment list.
/// </remarks>
public sealed class ConsumerGroupAckState {
    ImmutableSortedSet<ulong> _selectiveAcks = ImmutableSortedSet<ulong>.Empty;
    // A second, independent selective-ack set — fed only by Ack/AdvanceWatermarkTo, never by
    // ApplyLocally — so _committedWatermark can only ever collapse through offsets that are
    // themselves committed. AckIndex's own message dispatch (one message type per intent) is
    // what makes this separation structural now: ApplyLocally's handler has no way to reach this
    // field at all, not merely a discipline that a future change could quietly break again.
    ImmutableSortedSet<ulong> _committedSelectiveAcks = ImmutableSortedSet<ulong>.Empty;
    ulong _lowWatermark;       // "applied" — every locally-observed ack, committed or not
    ulong _committedWatermark; // only what a Raft quorum has actually agreed on

    /// <summary>
    /// Every offset below this one is acked, locally — committed or not. Zero means nothing has
    /// been acked yet. Drives client-facing decisions (redelivery avoidance) where a false
    /// negative only costs a redundant redelivery, never data loss.
    /// </summary>
    public ulong LowWatermark => Volatile.Read(ref _lowWatermark);

    /// <summary>
    /// Every offset below this one has been confirmed by an actual Raft quorum — never advanced
    /// by <see cref="ApplyLocally"/>. The only watermark pruning/dead-lettering may ever consult:
    /// a network-partitioned, isolated former leader has no way of discovering it lost quorum
    /// (the underlying consensus layer has no leader-lease/quorum-loss detection — it only steps
    /// down on observing a higher term from a peer it can actually reach), so it can keep believing itself
    /// leader, and keep accepting checkpoint acks via <see cref="ApplyLocally"/>, for as long as
    /// the partition lasts. If pruning consulted <see cref="LowWatermark"/> instead, that isolated
    /// node could physically delete or dead-letter data based on acks no quorum ever agreed to —
    /// silent, and because these watermarks never move backward, unrecoverable once the partition
    /// heals.
    /// </summary>
    public ulong CommittedWatermark => Volatile.Read(ref _committedWatermark);

    /// <summary>
    /// Whether <paramref name="offset"/> has been acked, via the watermark or selectively.
    /// </summary>
    public bool IsAcked(ulong offset) => offset < LowWatermark || Volatile.Read(ref _selectiveAcks).Contains(offset);

    /// <summary>
    /// Strict mode's replay path only: records every offset in <paramref name="offsets"/>,
    /// idempotently (a replayed or duplicated ack — at-least-once delivery — is a silent no-op,
    /// never an error and never a state change). Every offset here is, by construction, already
    /// Raft-committed — it only ever reaches this method via a committed <c>Ack</c> record — so
    /// this advances <see cref="CommittedWatermark"/> alongside <see cref="LowWatermark"/>.
    /// Collapses the watermark forward through any now-contiguous selective acks, the same
    /// consolidation TCP SACK performs. Only ever called from <see cref="AckIndex"/>'s own actor
    /// thread — never concurrently with itself or with <see cref="ApplyLocally"/>/
    /// <see cref="AdvanceWatermarkTo"/>, so no lock is needed here at all.
    /// </summary>
    internal void Ack(IEnumerable<ulong> offsets) {
        var selectiveAcks = _selectiveAcks;
        var committedSelectiveAcks = _committedSelectiveAcks;
        var lowWatermark = _lowWatermark;
        var committedWatermark = _committedWatermark;

        foreach (var offset in offsets) {
            if (offset >= lowWatermark)
                selectiveAcks = selectiveAcks.Add(offset);

            if (offset >= committedWatermark)
                committedSelectiveAcks = committedSelectiveAcks.Add(offset);
        }

        while (selectiveAcks.Contains(lowWatermark))
            selectiveAcks = selectiveAcks.Remove(lowWatermark++);

        while (committedSelectiveAcks.Contains(committedWatermark))
            committedSelectiveAcks = committedSelectiveAcks.Remove(committedWatermark++);

        Volatile.Write(ref _selectiveAcks, selectiveAcks);
        Volatile.Write(ref _committedSelectiveAcks, committedSelectiveAcks);
        Volatile.Write(ref _lowWatermark, lowWatermark);
        Volatile.Write(ref _committedWatermark, committedWatermark);
    }

    /// <summary>
    /// Checkpoint mode's cheap path only: applies <paramref name="offsets"/> immediately and
    /// locally, exactly like <see cref="Ack"/>'s watermark-collapsing, but WITHOUT ever advancing
    /// <see cref="CommittedWatermark"/> — this offset range has not gone through Raft, and might
    /// never (see <see cref="CommittedWatermark"/>'s own remarks on why that matters). Only ever
    /// called from <see cref="AckIndex"/>'s own actor thread, same as <see cref="Ack"/>.
    /// </summary>
    internal void ApplyLocally(IEnumerable<ulong> offsets) {
        var selectiveAcks = _selectiveAcks;
        var lowWatermark = _lowWatermark;

        foreach (var offset in offsets) {
            if (offset >= lowWatermark)
                selectiveAcks = selectiveAcks.Add(offset);
        }

        while (selectiveAcks.Contains(lowWatermark))
            selectiveAcks = selectiveAcks.Remove(lowWatermark++);

        Volatile.Write(ref _selectiveAcks, selectiveAcks);
        Volatile.Write(ref _lowWatermark, lowWatermark);
    }

    /// <summary>
    /// Checkpoint mode's periodic, committed catch-up — the only way <see cref="CommittedWatermark"/>
    /// ever advances for a checkpoint-mode group. Also floors <see cref="LowWatermark"/> up to the
    /// same value: <see cref="ApplyLocally"/>'s own progress never survives a restart (it was
    /// never persisted), so without this floor a recovering node would show a checkpoint group's
    /// <see cref="LowWatermark"/> stuck at zero despite a real, committed watermark far ahead. A
    /// no-op if <paramref name="watermark"/> is not ahead of the current committed one (a replayed
    /// or stale checkpoint — at-least-once, same idempotence discipline as <see cref="Ack"/>).
    /// Prunes any selective acks the new watermark now subsumes — a checkpoint only ever carries
    /// the contiguous low watermark, so any selective entry below it is now redundant bookkeeping
    /// that would otherwise never be reclaimed. Only ever called from <see cref="AckIndex"/>'s own
    /// actor thread, same as <see cref="Ack"/>.
    /// </summary>
    internal void AdvanceWatermarkTo(ulong watermark) {
        if (watermark <= _committedWatermark)
            return;

        Volatile.Write(ref _committedSelectiveAcks, ImmutableSortedSet.CreateRange(_committedSelectiveAcks.Where(offset => offset >= watermark)));
        Volatile.Write(ref _committedWatermark, watermark);

        if (watermark > _lowWatermark) {
            Volatile.Write(ref _selectiveAcks, ImmutableSortedSet.CreateRange(_selectiveAcks.Where(offset => offset >= watermark)));
            Volatile.Write(ref _lowWatermark, watermark);
        }
    }
}
