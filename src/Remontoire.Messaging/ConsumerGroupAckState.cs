namespace Remontoire.Messaging;

/// <summary>
/// One consumer-group's acknowledgment progress on one shard: a low watermark (every offset
/// below this one is acked; zero means nothing has been acked yet — offsets are 0-based, so an
/// inclusive "last acked offset" cannot distinguish "offset 0 is acked" from "nothing is acked",
/// the same reason this codebase's other progress counters, e.g. a shard log's own
/// next-offset-to-apply, are exclusive rather than inclusive) plus a small set of out-of-order,
/// individually acked offsets above it — the TCP-selective-acknowledgment pattern.
/// </summary>
public sealed class ConsumerGroupAckState {
    // Every mutation used to run exclusively on AckIndexApplier's own sequential replay loop — a
    // single writer, so no synchronization was needed. Checkpoint-mode acks apply directly from
    // concurrent gRPC threads via AckIndex.ApplyLocal, bypassing that loop entirely — a second,
    // genuinely concurrent writer, which is what this lock now guards against.
    readonly Lock _gate = new();
    readonly SortedSet<ulong> _selectiveAcks = [];
    // A second, independent selective-ack set — fed only by Ack/AdvanceWatermarkTo, never by
    // ApplyLocally — so _committedWatermark can only ever collapse through offsets that are
    // themselves committed. Sharing _selectiveAcks/_lowWatermark between both writers would let a
    // later Ack call promote _lowWatermark (however it got that far ahead, including via
    // ApplyLocally) straight into _committedWatermark — exactly the non-deterministic-across-nodes
    // state this split exists to rule out.
    readonly SortedSet<ulong> _committedSelectiveAcks = [];
    ulong _lowWatermark;       // "applied" — every locally-observed ack, committed or not
    ulong _committedWatermark; // only what a Raft quorum has actually agreed on

    /// <summary>
    /// Every offset below this one is acked, locally — committed or not. Zero means nothing has
    /// been acked yet. Drives client-facing decisions (redelivery avoidance) where a false
    /// negative only costs a redundant redelivery, never data loss.
    /// </summary>
    public ulong LowWatermark {
        get { lock (_gate) return _lowWatermark; }
    }

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
    public ulong CommittedWatermark {
        get { lock (_gate) return _committedWatermark; }
    }

    /// <summary>
    /// Whether <paramref name="offset"/> has been acked, via the watermark or selectively.
    /// </summary>
    public bool IsAcked(ulong offset) { lock (_gate) return offset < _lowWatermark || _selectiveAcks.Contains(offset); }

    /// <summary>
    /// Strict mode's replay path only: records every offset in <paramref name="offsets"/>,
    /// idempotently (a replayed or duplicated ack — at-least-once delivery — is a silent no-op,
    /// never an error and never a state change). Every offset here is, by construction, already
    /// Raft-committed — it only ever reaches this method via a committed <c>Ack</c> record — so
    /// this advances <see cref="CommittedWatermark"/> alongside <see cref="LowWatermark"/>.
    /// Collapses the watermark forward through any now-contiguous selective acks, the same
    /// consolidation TCP SACK performs. Takes the whole batch under a single lock acquisition
    /// rather than one per offset — a real, avoidable cost on checkpoint mode's concurrent-gRPC-
    /// thread path, which no longer has Raft's round-trip to mask lock churn.
    /// </summary>
    public void Ack(IEnumerable<ulong> offsets) {
        lock (_gate) {
            foreach (var offset in offsets) {
                if (offset >= _lowWatermark)
                    _selectiveAcks.Add(offset);

                if (offset >= _committedWatermark)
                    _committedSelectiveAcks.Add(offset);
            }

            while (_selectiveAcks.Remove(_lowWatermark))
                _lowWatermark++;

            while (_committedSelectiveAcks.Remove(_committedWatermark))
                _committedWatermark++;
        }
    }

    /// <summary>
    /// Checkpoint mode's cheap path only: applies <paramref name="offsets"/> immediately and
    /// locally, exactly like <see cref="Ack"/>'s watermark-collapsing, but WITHOUT ever advancing
    /// <see cref="CommittedWatermark"/> — this offset range has not gone through Raft, and might
    /// never (see <see cref="CommittedWatermark"/>'s own remarks on why that matters).
    /// </summary>
    public void ApplyLocally(IEnumerable<ulong> offsets) {
        lock (_gate) {
            foreach (var offset in offsets) {
                if (offset < _lowWatermark)
                    continue;

                _selectiveAcks.Add(offset);
            }

            while (_selectiveAcks.Remove(_lowWatermark))
                _lowWatermark++;
        }
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
    /// that would otherwise never be reclaimed.
    /// </summary>
    public void AdvanceWatermarkTo(ulong watermark) {
        lock (_gate) {
            if (watermark <= _committedWatermark)
                return;

            _committedWatermark = watermark;
            _committedSelectiveAcks.RemoveWhere(offset => offset < watermark);

            if (watermark > _lowWatermark) {
                _selectiveAcks.RemoveWhere(offset => offset < watermark);
                _lowWatermark = watermark;
            }
        }
    }
}
