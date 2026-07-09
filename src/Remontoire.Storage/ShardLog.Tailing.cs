namespace Remontoire.Storage;

public sealed partial class ShardLog {
    // Pure forwarder — the injected committed-source hands off each record in the exact order it
    // committed, so this loop never needs to re-derive order itself. Just relay into the shared
    // mailbox as a WalRecordCommitted message; the actor loop (ShardLog.cs's RunActorAsync →
    // HandleWalRecordCommittedAsync, ShardLog.Apply.cs) is the only place that touches
    // _memTable/_segments. The source is owned by its caller, not by this class — cancelling
    // `_tailingCts` (ShardLog.DisposeAsync) is the only way this loop ever ends.
    async Task RunTailingLoopAsync() {
        try {
            await foreach (var record in _committedSource(_tailingCts.Token))
                _mailbox.Writer.TryWrite(new WalRecordCommitted(record));
        } catch (OperationCanceledException) {
            // Expected shutdown path — ShardLog.DisposeAsync cancels _tailingCts and awaits this.
        }
    }
}
