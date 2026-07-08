using System.Text;
using FluentAssertions;

namespace Remontoire.Storage;

public class ShardLogTests {
    public class AppendAsync {
        [Fact]
        public async Task Assigns_sequential_offsets_starting_at_zero() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory);

                (await log.AppendAsync(SampleRequest())).Should().Be(0);
                (await log.AppendAsync(SampleRequest())).Should().Be(1);
                (await log.AppendAsync(SampleRequest())).Should().Be(2);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Assigned_offsets_stay_sequential_under_concurrent_callers() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory);

                var offsets = await Task.WhenAll(Enumerable.Range(0, 50).Select(_ => log.AppendAsync(SampleRequest()).AsTask()));

                offsets.OrderBy(o => o).Should().Equal(Enumerable.Range(0, 50).Select(i => (ulong)i));
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class DisposeAsync {
        [Fact]
        public async Task Completes_an_already_accepted_append_instead_of_abandoning_it() {
            var directory = CreateTempDirectory();
            try {
                var log = await ShardLog.OpenAsync(directory);
                var appendTask = log.AppendAsync(SampleRequest()).AsTask();

                await log.DisposeAsync();

                (await appendTask).Should().Be(0);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Every_accepted_append_survives_a_dispose_that_starts_mid_flight() {
            var directory = CreateTempDirectory();
            try {
                var log = await ShardLog.OpenAsync(directory);
                var appendTasks = Enumerable.Range(0, 50).Select(_ => log.AppendAsync(SampleRequest()).AsTask()).ToArray();

                await log.DisposeAsync();
                var offsets = await Task.WhenAll(appendTasks);

                offsets.OrderBy(o => o).Should().Equal(Enumerable.Range(0, 50).Select(i => (ulong)i));

                await using var reopened = await ShardLog.OpenAsync(directory);
                foreach (var offset in offsets) {
                    reopened.TryGet(offset, out var handle).Should().BeTrue();
                    handle.Dispose();
                }
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class TryGet {
        [Fact]
        public async Task Returns_false_for_an_offset_never_appended() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory);

                log.TryGet(0, out _).Should().BeFalse();
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Eventually_finds_an_appended_entry() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory);
                var offset = await log.AppendAsync(SampleRequest(payload: "hello world"));

                var entry = await WaitForVisibleAsync(log, offset);

                Encoding.UTF8.GetString(entry.Payload.Span).Should().Be("hello world");
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class ReadFromAsync {
        [Fact]
        public async Task Yields_appended_entries_in_order() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory);
                for (var i = 0; i < 5; i++)
                    await log.AppendAsync(SampleRequest());

                await WaitForVisibleAsync(log, 4);

                var offsets = new List<ulong>();
                await foreach (var handle in log.ReadFromAsync(0))
                    using (handle)
                        offsets.Add(handle.Entry.LogicalOffset);

                offsets.Should().Equal(0ul, 1ul, 2ul, 3ul, 4ul);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class Flushing {
        [Fact]
        public async Task Creates_a_segment_per_entry_when_the_threshold_is_tiny_and_all_entries_stay_readable() {
            var directory = CreateTempDirectory();
            try {
                await using (var log = await ShardLog.OpenAsync(directory, flushThresholdBytes: 1)) {
                    for (var i = 0; i < 3; i++)
                        await log.AppendAsync(SampleRequest());

                    await WaitForVisibleAsync(log, 2);

                    for (ulong offset = 0; offset < 3; offset++) {
                        log.TryGet(offset, out var handle).Should().BeTrue();
                        using (handle)
                            handle.Entry.LogicalOffset.Should().Be(offset);
                    }
                }

                Directory.GetFiles(directory, "*.sst").Should().HaveCount(3);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class Compaction {
        [Fact]
        public async Task Merges_flushed_segments_down_to_one_and_keeps_every_entry_readable() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(
                    directory, flushThresholdBytes: 1, compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null));

                const int count = 5;
                for (var i = 0; i < count; i++)
                    await log.AppendAsync(SampleRequest(payload: $"entry-{i}"));

                await WaitForVisibleAsync(log, count - 1);
                await WaitForSegmentCountAsync(directory, 1);

                for (ulong offset = 0; offset < count; offset++) {
                    log.TryGet(offset, out var handle).Should().BeTrue();
                    using (handle)
                        Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be($"entry-{offset}");
                }
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Disposing_with_a_compaction_policy_set_but_nothing_to_compact_does_not_hang() {
            var directory = CreateTempDirectory();
            try {
                var log = await ShardLog.OpenAsync(directory, compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null));
                await log.AppendAsync(SampleRequest());

                await log.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class Recovery {
        [Fact]
        public async Task Restarting_replays_identical_data_from_the_WAL_tail() {
            var directory = CreateTempDirectory();
            try {
                await using (var log = await ShardLog.OpenAsync(directory)) {
                    for (var i = 0; i < 5; i++)
                        await log.AppendAsync(SampleRequest(payload: $"entry-{i}"));
                }

                await using var reopened = await ShardLog.OpenAsync(directory);

                for (ulong offset = 0; offset < 5; offset++) {
                    reopened.TryGet(offset, out var handle).Should().BeTrue();
                    using (handle) {
                        handle.Entry.LogicalOffset.Should().Be(offset);
                        Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be($"entry-{offset}");
                    }
                }
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Restarting_after_a_flush_still_finds_segment_backed_entries() {
            var directory = CreateTempDirectory();
            try {
                await using (var log = await ShardLog.OpenAsync(directory, flushThresholdBytes: 1)) {
                    for (var i = 0; i < 3; i++)
                        await log.AppendAsync(SampleRequest(payload: $"entry-{i}"));

                    await WaitForVisibleAsync(log, 2);
                }

                await using var reopened = await ShardLog.OpenAsync(directory, flushThresholdBytes: 1);

                for (ulong offset = 0; offset < 3; offset++) {
                    reopened.TryGet(offset, out var handle).Should().BeTrue();
                    using (handle)
                        Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be($"entry-{offset}");
                }

                // appending after reopening must continue from the correct next offset, not restart at 0
                (await reopened.AppendAsync(SampleRequest())).Should().Be(3);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Recovers_a_segment_left_behind_by_an_interrupted_compaction() {
            var directory = CreateTempDirectory();
            try {
                // Simulates a crash between a compaction's "delete old inputs" and "rename to
                // final name" steps: the merged content is already fully durable under a
                // ".merging" name, and (per that recovery design) the segments it replaced are
                // already gone — nothing else needs to exist for this to be a valid scenario.
                var mergingPath = Path.Combine(directory, $"segment-{0:D20}.sst.merging");
                await SstWriter.WriteAsync(mergingPath, [
                    new LogEntry(0, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes("entry-0")),
                    new LogEntry(1, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes("entry-1")),
                ]);

                await using var log = await ShardLog.OpenAsync(directory);

                Directory.GetFiles(directory, "*.sst.merging").Should().BeEmpty();
                Directory.GetFiles(directory, "*.sst").Should().ContainSingle();

                for (ulong offset = 0; offset < 2; offset++) {
                    log.TryGet(offset, out var handle).Should().BeTrue();
                    using (handle)
                        Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be($"entry-{offset}");
                }

                // appending after recovery must continue from offset 2, not restart at 0
                (await log.AppendAsync(SampleRequest())).Should().Be(2);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class FaultHandling {
        [Fact]
        public async Task A_WAL_write_failure_faults_the_append_without_crashing_the_actor() {
            var directory = CreateTempDirectory();
            try {
                var stream = new ThrowingFlushFileStream(Path.Combine(directory, "wal.log"));
                var walWriter = new WalWriter(stream);

                await using var log = new ShardLog(directory, walWriter, new MemTable(), [], nextLogicalOffset: 0, flushThresholdBytes: 64 * 1024 * 1024);

                var act = async () => await log.AppendAsync(SampleRequest());
                await act.Should().ThrowAsync<IOException>();

                // the actor loop must still be alive afterward — a second append also fails
                // cleanly (not hangs), proving one faulted append doesn't take the actor down
                var act2 = async () => await log.AppendAsync(SampleRequest()).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                await act2.Should().ThrowAsync<IOException>();
            } finally {
                Directory.Delete(directory, recursive: true);
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
    }

    public class Concurrency {
        [Fact]
        public async Task An_entry_never_becomes_invisible_again_once_visible_across_many_flushes() {
            var directory = CreateTempDirectory();
            try {
                // A tiny threshold forces a flush after nearly every append, maximizing the
                // chance of hitting the publish-order window (new segment published, THEN the
                // old MemTable retracted) if that ordering were ever wrong.
                await using var log = await ShardLog.OpenAsync(directory, flushThresholdBytes: 60);

                const int count = 100;
                var offsets = new ulong[count];
                for (var i = 0; i < count; i++)
                    offsets[i] = await log.AppendAsync(SampleRequest());

                var stop = 0;
                var flickered = 0;

                var reader = Task.Run(() => {
                    var everVisible = new bool[count];
                    while (Volatile.Read(ref stop) == 0) {
                        for (var i = 0; i < count; i++) {
                            var found = log.TryGet(offsets[i], out var handle);
                            if (found)
                                handle.Dispose();

                            if (found)
                                everVisible[i] = true;
                            else if (everVisible[i])
                                Interlocked.Exchange(ref flickered, 1); // was visible, now isn't — the bug this guards against
                        }
                    }
                });

                await WaitForVisibleAsync(log, offsets[^1]);
                Interlocked.Exchange(ref stop, 1);
                await reader;

                flickered.Should().Be(0);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    static string CreateTempDirectory() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }

    static async Task<LogEntry> WaitForVisibleAsync(ShardLog log, ulong offset) {
        for (var i = 0; i < 500; i++) {
            if (log.TryGet(offset, out var handle))
                using (handle)
                    return handle.Entry;

            await Task.Delay(10);
        }

        throw new TimeoutException($"Offset {offset} never became visible.");
    }

    static async Task WaitForSegmentCountAsync(string directory, int expected) {
        for (var i = 0; i < 500; i++) {
            if (Directory.GetFiles(directory, "*.sst").Length == expected)
                return;

            await Task.Delay(10);
        }

        throw new TimeoutException($"Segment count never reached {expected}.");
    }

    static AppendRequest SampleRequest(string payload = "hello world") =>
        new(Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes(payload));
}
