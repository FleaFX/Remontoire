namespace Remontoire.Storage;

public sealed partial class ShardLog {
    // Pure forwarder — WalWriter.ReadDurableAsync hands off each record the moment it's
    // durable, in the exact order its own single, sequential commit loop wrote it, so this
    // loop never needs to re-derive order itself. Just relay into the shared mailbox as a
    // WalRecordCommitted message; the actor loop (ShardLog.cs's RunActorAsync →
    // HandleWalRecordCommittedAsync, ShardLog.Apply.cs) is the only place that touches
    // _memTable/_segments. Ends naturally (no cancellation needed) once WalWriter.DisposeAsync
    // completes its ReadDurableAsync stream (ShardLog.DisposeAsync).
    async Task RunTailingLoopAsync() {
        await foreach (var record in _walWriter.ReadDurableAsync())
            _mailbox.Writer.TryWrite(new WalRecordCommitted(record));
    }
}
