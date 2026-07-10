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
    readonly SortedSet<ulong> _selectiveAcks = [];
    ulong _lowWatermark;

    /// <summary>
    /// Every offset below this one is acked. Zero means nothing has been acked yet.
    /// </summary>
    public ulong LowWatermark => _lowWatermark;

    /// <summary>
    /// Whether <paramref name="offset"/> has been acked, via the watermark or selectively.
    /// </summary>
    public bool IsAcked(ulong offset) => offset < _lowWatermark || _selectiveAcks.Contains(offset);

    /// <summary>
    /// Records one acked offset. Idempotent: re-acking an already-covered offset (a replayed or
    /// duplicated Ack record — at-least-once delivery) is a silent no-op, never an error and never
    /// a state change. Collapses the watermark forward through any now-contiguous selective acks,
    /// the same consolidation TCP SACK performs.
    /// </summary>
    public void Ack(ulong offset) {
        if (offset < _lowWatermark)
            return;

        _selectiveAcks.Add(offset);
        while (_selectiveAcks.Remove(_lowWatermark))
            _lowWatermark++;
    }
}
