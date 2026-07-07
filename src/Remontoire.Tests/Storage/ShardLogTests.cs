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

    static AppendRequest SampleRequest(string payload = "hello world") =>
        new(Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes(payload));
}
