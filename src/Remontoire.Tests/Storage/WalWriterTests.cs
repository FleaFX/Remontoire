using System.Text;
using FluentAssertions;
using Remontoire.Storage.Serialization;

namespace Remontoire.Storage;

public class WalWriterTests {
    public class AppendAsync {
        [Fact]
        public async Task Writes_a_record_that_round_trips_from_disk() {
            var path = Path.GetTempFileName();
            try {
                var record = SampleRecord(logicalOffset: 7);

                var writer = await WalWriter.OpenAsync(path);
                await writer.AppendAsync(record);
                await writer.DisposeAsync();

                var bytes = await File.ReadAllBytesAsync(path);
                using var result = WalRecordSerializer.TryRead(bytes);

                result.Status.Should().Be(WalRecordReadStatus.Success);
                result.Record.LogicalOffset.Should().Be(7);
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Does_not_complete_before_the_batch_is_fsynced() {
            var path = Path.GetTempFileName();
            try {
                var stream = new GatedFlushFileStream(path);
                var writer = new WalWriter(stream);
                try {
                    var appendTask = writer.AppendAsync(SampleRecord()).AsTask();

                    await stream.FlushStarted.Task;
                    appendTask.IsCompleted.Should().BeFalse();

                    stream.FlushGate.TrySetResult();
                    await appendTask;
                } finally {
                    stream.FlushGate.TrySetResult();
                    await writer.DisposeAsync();
                }
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Batches_concurrent_appends_into_fewer_fsyncs_than_appends() {
            var path = Path.GetTempFileName();
            try {
                var stream = new CountingFlushFileStream(path);
                var writer = new WalWriter(stream);
                try {
                    var tasks = Enumerable.Range(0, 200)
                        .Select(i => writer.AppendAsync(SampleRecord((ulong)i)).AsTask())
                        .ToArray();

                    await Task.WhenAll(tasks);

                    stream.FlushToDiskCount.Should().BeLessThan(tasks.Length);
                } finally {
                    await writer.DisposeAsync();
                }
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Cancelling_the_wait_does_not_abort_the_underlying_write() {
            var path = Path.GetTempFileName();
            try {
                var stream = new GatedFlushFileStream(path);
                var writer = new WalWriter(stream);
                try {
                    using var cts = new CancellationTokenSource();
                    var appendTask = writer.AppendAsync(SampleRecord(logicalOffset: 9), cts.Token).AsTask();

                    await stream.FlushStarted.Task; // the record is already enqueued; the fsync is gated
                    cts.Cancel();

                    var act = async () => await appendTask;
                    await act.Should().ThrowAsync<OperationCanceledException>();

                    stream.FlushGate.TrySetResult(); // let the actual write/fsync proceed regardless
                } finally {
                    stream.FlushGate.TrySetResult();
                    await writer.DisposeAsync();
                }

                var bytes = await File.ReadAllBytesAsync(path);
                using var result = WalRecordSerializer.TryRead(bytes);
                result.Status.Should().Be(WalRecordReadStatus.Success);
                result.Record.LogicalOffset.Should().Be(9);
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Throws_ObjectDisposedException_after_dispose() {
            var path = Path.GetTempFileName();
            try {
                var writer = await WalWriter.OpenAsync(path);
                await writer.DisposeAsync();

                Action act = () => { _ = writer.AppendAsync(SampleRecord()); };

                act.Should().Throw<ObjectDisposedException>();
            } finally {
                File.Delete(path);
            }
        }
    }

    public class FaultHandling {
        [Fact]
        public async Task A_batch_write_failure_faults_every_pending_append_in_that_batch() {
            var path = Path.GetTempFileName();
            try {
                var stream = new ThrowingFlushFileStream(path);
                var writer = new WalWriter(stream);
                try {
                    var tasks = Enumerable.Range(0, 5).Select(i => writer.AppendAsync(SampleRecord((ulong)i)).AsTask()).ToArray();

                    foreach (var task in tasks) {
                        var act = async () => await task;
                        await act.Should().ThrowAsync<IOException>();
                    }
                } finally {
                    await writer.DisposeAsync();
                }
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task A_failed_batchs_bytes_never_become_durable_via_a_later_batchs_flush() {
            var path = Path.GetTempFileName();
            try {
                var stream = new FlushFailsOnceFileStream(path);
                var writer = new WalWriter(stream);
                try {
                    var act = async () => await writer.AppendAsync(SampleRecord(logicalOffset: 1));
                    await act.Should().ThrowAsync<IOException>();

                    await writer.AppendAsync(SampleRecord(logicalOffset: 2)); // succeeds, flushes the whole file
                } finally {
                    await writer.DisposeAsync();
                }

                var reader = new WalReader(path);
                var offsets = new List<ulong>();
                await foreach (var result in reader.ReadFromAsync())
                    using (result)
                        offsets.Add(result.Record.LogicalOffset);

                // Record 1's bytes must never resurface, even though they were already written to
                // the file before its batch's flush failed — the later, successful flush for
                // record 2 must not have promoted them to durable along the way.
                offsets.Should().Equal(2ul);
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task A_write_failure_that_cannot_be_truncated_away_permanently_faults_the_writer() {
            var path = Path.GetTempFileName();
            try {
                var stream = new UntruncatableFileStream(path);
                var writer = new WalWriter(stream);
                try {
                    var act1 = async () => await writer.AppendAsync(SampleRecord(logicalOffset: 1));
                    await act1.Should().ThrowAsync<IOException>();

                    // The writer is now permanently faulted — a second append must fail
                    // immediately, not hang waiting on a commit loop that has stopped processing.
                    var act2 = async () => await writer.AppendAsync(SampleRecord(logicalOffset: 2)).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                    await act2.Should().ThrowAsync<IOException>();
                } finally {
                    await writer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                }
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task A_batch_write_failure_publishes_nothing_to_ReadCommittedAsync() {
            var path = Path.GetTempFileName();
            try {
                var stream = new ThrowingFlushFileStream(path);
                var writer = new WalWriter(stream);
                var committed = new List<ulong>();
                var reading = Task.Run(async () => {
                    await foreach (var record in writer.ReadCommittedAsync())
                        committed.Add(record.LogicalOffset);
                });

                var act = async () => await writer.AppendAsync(SampleRecord());
                await act.Should().ThrowAsync<IOException>();

                await writer.DisposeAsync();
                await reading;

                committed.Should().BeEmpty();
            } finally {
                File.Delete(path);
            }
        }
    }

    public class DisposeAsync {
        [Fact]
        public async Task Completes_already_queued_appends_instead_of_rejecting_them() {
            var path = Path.GetTempFileName();
            try {
                var writer = await WalWriter.OpenAsync(path);
                var appendTask = writer.AppendAsync(SampleRecord()).AsTask();

                await writer.DisposeAsync();

                await appendTask;
            } finally {
                File.Delete(path);
            }
        }
    }

    public class ReadCommittedAsync {
        [Fact]
        public async Task Yields_each_record_in_commit_order() {
            var path = Path.GetTempFileName();
            try {
                var writer = await WalWriter.OpenAsync(path);
                var committed = new List<ulong>();
                var reading = Task.Run(async () => {
                    await foreach (var record in writer.ReadCommittedAsync())
                        committed.Add(record.LogicalOffset);
                });

                await writer.AppendAsync(SampleRecord(1));
                await writer.AppendAsync(SampleRecord(2));
                await writer.AppendAsync(SampleRecord(3));

                await writer.DisposeAsync(); // completes the commit loop, and so ReadCommittedAsync's enumeration
                await reading;

                committed.Should().Equal(1ul, 2ul, 3ul);
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Yields_the_same_records_from_a_batch_that_were_just_completed() {
            var path = Path.GetTempFileName();
            try {
                var stream = new CountingFlushFileStream(path);
                var writer = new WalWriter(stream);
                var committed = new List<ulong>();
                var reading = Task.Run(async () => {
                    await foreach (var record in writer.ReadCommittedAsync())
                        committed.Add(record.LogicalOffset);
                });

                var tasks = Enumerable.Range(0, 50).Select(i => writer.AppendAsync(SampleRecord((ulong)i)).AsTask());
                await Task.WhenAll(tasks);

                await writer.DisposeAsync();
                await reading;

                committed.Should().Equal(Enumerable.Range(0, 50).Select(i => (ulong)i));
            } finally {
                File.Delete(path);
            }
        }
    }

    static WalRecord SampleRecord(ulong logicalOffset = 1) =>
        new(WalRecordType.Append, RaftTerm: 0, RaftIndex: 0, logicalOffset, TimestampMicros: 42,
            PartitionKey: Encoding.UTF8.GetBytes("order-42"), Headers: [], Payload: Encoding.UTF8.GetBytes("hello world"));

    sealed class CountingFlushFileStream(string path)
        : FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0, useAsync: true) {
        public int FlushToDiskCount { get; private set; }

        public override void Flush(bool flushToDisk) {
            if (flushToDisk)
                FlushToDiskCount++;

            base.Flush(flushToDisk);
        }
    }

    sealed class ThrowingFlushFileStream(string path)
        : FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0, useAsync: true) {
        public override void Flush(bool flushToDisk) {
            if (flushToDisk)
                throw new IOException("Simulated disk failure.");

            base.Flush(flushToDisk);
        }
    }

    sealed class FlushFailsOnceFileStream(string path)
        : FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0, useAsync: true) {
        bool _hasFailedOnce;

        public override void Flush(bool flushToDisk) {
            if (flushToDisk && !_hasFailedOnce) {
                _hasFailedOnce = true;
                throw new IOException("Simulated transient disk failure.");
            }

            base.Flush(flushToDisk);
        }
    }

    // Simulates a device that fails a flush exactly once (recoverable, in principle) but can
    // never truncate — e.g. an append-only/log-structured device. Lets a test distinguish "this
    // specific write failed" from "the writer is now permanently unusable": if the writer weren't
    // escalating to a permanent fault, a second append would succeed outright (this stream's one
    // and only forced Flush failure is already spent), instead of failing immediately.
    sealed class UntruncatableFileStream(string path)
        : FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0, useAsync: true) {
        bool _hasFailedOnce;

        public override void Flush(bool flushToDisk) {
            if (flushToDisk && !_hasFailedOnce) {
                _hasFailedOnce = true;
                throw new IOException("Simulated disk failure.");
            }

            base.Flush(flushToDisk);
        }

        public override void SetLength(long value) =>
            throw new IOException("Simulated unrecoverable disk failure — cannot even truncate.");
    }

    sealed class GatedFlushFileStream(string path)
        : FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0, useAsync: true) {
        public TaskCompletionSource FlushStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FlushGate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override void Flush(bool flushToDisk) {
            if (flushToDisk) {
                FlushStarted.TrySetResult();
                FlushGate.Task.GetAwaiter().GetResult();
            }

            base.Flush(flushToDisk);
        }
    }
}
