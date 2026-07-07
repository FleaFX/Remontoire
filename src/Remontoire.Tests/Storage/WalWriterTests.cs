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
