using System.Runtime.CompilerServices;
using Remontoire.Storage;

namespace Remontoire.Server;

/// <summary>
/// Buffers every committed meta-log record (version + encoded payload + commit timestamp + wire
/// headers) this node has seen, and fans it out to any number of concurrent <see cref="WatchAsync"/>
/// readers — unlike the single-reader committed-record stream it's fed from, several independent
/// network subscribers (each an in-progress <c>Watch</c> RPC call) need the same records at once.
/// Admin-command volume is tiny compared to message volume, so keeping the full history in memory
/// forever, with no pruning, is the deliberate, simple choice here.
/// </summary>
public sealed class MetaLogJournal {
    readonly object _gate = new();
    readonly List<(ulong Version, byte[] Payload, ulong TimestampMicros, IReadOnlyList<Header> Headers)> _records = [];
    volatile TaskCompletionSource _appended = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Records one more committed entry and wakes every current <see cref="WatchAsync"/> waiter.
    /// </summary>
    public void Append(ulong version, byte[] payload, ulong timestampMicros, IReadOnlyList<Header> headers) {
        TaskCompletionSource previouslyAppended;
        lock (_gate) {
            _records.Add((version, payload, timestampMicros, headers));
            previouslyAppended = _appended;
            _appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        previouslyAppended.SetResult();
    }

    /// <summary>
    /// The full history so far, as of this call, plus the version it reflects (0 if empty).
    /// </summary>
    public (ulong Version, IReadOnlyList<(ulong Version, byte[] Payload, ulong TimestampMicros, IReadOnlyList<Header> Headers)> Records) Snapshot() {
        lock (_gate)
            return (_records.Count == 0 ? 0 : _records[^1].Version, _records.ToArray());
    }

    /// <summary>
    /// Yields every record from <paramref name="fromVersionInclusive"/> onward (itself included)
    /// committed so far, then keeps yielding new ones as they arrive — never terminates on its own.
    /// Deliberately inclusive, not "everything after the last one I've seen": versions are Raft
    /// LogicalOffsets, 0-based, so the very first record a group ever commits genuinely has version
    /// 0 — an EXCLUSIVE "greater than" bound could never distinguish a caller that has already seen
    /// version 0 from one asking "from the very beginning," both passing 0, and would silently,
    /// permanently skip that first record for the latter.
    /// </summary>
    public async IAsyncEnumerable<(ulong Version, byte[] Payload, ulong TimestampMicros, IReadOnlyList<Header> Headers)> WatchAsync(
        ulong fromVersionInclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        while (true) {
            TaskCompletionSource signal;
            (ulong Version, byte[] Payload, ulong TimestampMicros, IReadOnlyList<Header> Headers)[] batch;
            lock (_gate) {
                signal = _appended;
                batch = _records.Where(record => record.Version >= fromVersionInclusive).ToArray();
            }

            foreach (var record in batch) {
                yield return record;
                fromVersionInclusive = record.Version + 1;
            }

            if (batch.Length == 0)
                await signal.Task.WaitAsync(cancellationToken);
        }
    }
}
