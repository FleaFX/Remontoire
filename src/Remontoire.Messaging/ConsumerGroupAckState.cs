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
    ulong _lowWatermark;

    /// <summary>
    /// Every offset below this one is acked. Zero means nothing has been acked yet.
    /// </summary>
    public ulong LowWatermark { get { lock (_gate) return _lowWatermark; } }

    /// <summary>
    /// Whether <paramref name="offset"/> has been acked, via the watermark or selectively.
    /// </summary>
    public bool IsAcked(ulong offset) { lock (_gate) return offset < _lowWatermark || _selectiveAcks.Contains(offset); }

    /// <summary>
    /// Records every offset in <paramref name="offsets"/>, idempotently (a replayed or duplicated
    /// ack — at-least-once delivery — is a silent no-op, never an error and never a state change).
    /// Collapses the watermark forward through any now-contiguous selective acks, the same
    /// consolidation TCP SACK performs. Takes the whole batch under a single lock acquisition
    /// rather than one per offset — a real, avoidable cost on checkpoint mode's concurrent-gRPC-
    /// thread path, which no longer has Raft's round-trip to mask lock churn.
    /// </summary>
    public void Ack(IEnumerable<ulong> offsets) {
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
    /// Advances the watermark directly to <paramref name="watermark"/> — checkpoint mode's own
    /// apply path, distinct from <see cref="Ack"/>'s selective-offset collapsing. A no-op if
    /// <paramref name="watermark"/> is not ahead of the current one (a replayed or stale
    /// checkpoint — at-least-once, same idempotence discipline as <see cref="Ack"/>). Prunes any
    /// selective acks the new watermark now subsumes — a checkpoint only ever carries the
    /// contiguous low watermark, so any selective entry below it is now redundant bookkeeping that
    /// would otherwise never be reclaimed.
    /// </summary>
    public void AdvanceWatermarkTo(ulong watermark) {
        lock (_gate) {
            if (watermark <= _lowWatermark)
                return;

            _selectiveAcks.RemoveWhere(offset => offset < watermark);
            _lowWatermark = watermark;
        }
    }
}
