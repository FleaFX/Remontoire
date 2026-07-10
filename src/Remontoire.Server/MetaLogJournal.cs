using System.Runtime.CompilerServices;

namespace Remontoire.Server;

/// <summary>
/// Buffers every committed meta-log record (version + encoded payload) this node has seen, and
/// fans it out to any number of concurrent <see cref="WatchAsync"/> readers — unlike the
/// single-reader committed-record stream it's fed from, several independent network subscribers
/// (each an in-progress <c>Watch</c> RPC call) need the same records at once. Admin-command
/// volume is tiny compared to message volume, so keeping the full history in memory forever,
/// with no pruning, is the deliberate, simple choice here.
/// </summary>
public sealed class MetaLogJournal {
    readonly object _gate = new();
    readonly List<(ulong Version, byte[] Payload)> _records = [];
    volatile TaskCompletionSource _appended = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Records one more committed entry and wakes every current <see cref="WatchAsync"/> waiter.
    /// </summary>
    public void Append(ulong version, byte[] payload) {
        TaskCompletionSource previouslyAppended;
        lock (_gate) {
            _records.Add((version, payload));
            previouslyAppended = _appended;
            _appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        previouslyAppended.SetResult();
    }

    /// <summary>
    /// The full history so far, as of this call, plus the version it reflects (0 if empty).
    /// </summary>
    public (ulong Version, IReadOnlyList<(ulong Version, byte[] Payload)> Records) Snapshot() {
        lock (_gate)
            return (_records.Count == 0 ? 0 : _records[^1].Version, _records.ToArray());
    }

    /// <summary>
    /// Yields every record after <paramref name="fromVersion"/> committed so far, then keeps
    /// yielding new ones as they arrive — never terminates on its own.
    /// </summary>
    public async IAsyncEnumerable<(ulong Version, byte[] Payload)> WatchAsync(
        ulong fromVersion, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        while (true) {
            TaskCompletionSource signal;
            (ulong Version, byte[] Payload)[] batch;
            lock (_gate) {
                signal = _appended;
                batch = _records.Where(record => record.Version > fromVersion).ToArray();
            }

            foreach (var record in batch) {
                yield return record;
                fromVersion = record.Version;
            }

            if (batch.Length == 0)
                await signal.Task.WaitAsync(cancellationToken);
        }
    }
}
