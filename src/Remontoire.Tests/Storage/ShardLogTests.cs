using System.Runtime.CompilerServices;
using System.Text;
using FluentAssertions;

namespace Remontoire.Storage;

public class ShardLogTests {
    public class DisposeAsync {
        // ShardLog no longer owns any durable storage of its own (that moved to WalRaftLog) —
        // a record only survives a restart once it reached a segment. What DisposeAsync itself
        // must still guarantee is that every message already sitting in the mailbox before it's
        // called gets applied before it returns, rather than being abandoned mid-flight.

        [Fact]
        public async Task Applies_a_record_already_posted_before_dispose_is_called() {
            var directory = CreateTempDirectory();
            try {
                var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
                log.TryPost(new WalRecordCommitted(SampleRecord(0)));

                await log.DisposeAsync();

                log.TryGet(0, out var handle).Should().BeTrue();
                handle.Dispose();
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Every_posted_record_survives_a_dispose_that_starts_mid_flight() {
            var directory = CreateTempDirectory();
            try {
                var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
                for (ulong i = 0; i < 50; i++)
                    log.TryPost(new WalRecordCommitted(SampleRecord(i)));

                await log.DisposeAsync();

                for (ulong i = 0; i < 50; i++) {
                    log.TryGet(i, out var handle).Should().BeTrue();
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
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);

                log.TryGet(0, out _).Should().BeFalse();
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Eventually_finds_an_appended_entry() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
                log.TryPost(new WalRecordCommitted(SampleRecord(0, payload: "hello world")));

                var entry = await WaitForVisibleAsync(log, 0);

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
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
                for (ulong i = 0; i < 5; i++)
                    log.TryPost(new WalRecordCommitted(SampleRecord(i)));

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

        [Fact]
        public async Task Does_not_skip_an_entry_that_a_concurrent_flush_moves_from_MemTable_into_a_new_segment() {
            var directory = CreateTempDirectory();
            try {
                var segmentPath = Path.Combine(directory, $"segment-{0:D20}.sst");
                await SstWriter.WriteAsync(segmentPath, [
                    new LogEntry(0, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes("entry-0")),
                ]);
                var segmentA = await SstSegment.OpenAsync(segmentPath);

                var memTable = new MemTable();
                memTable.Append(new LogEntry(1, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes("entry-1")));

                await using var log = new ShardLog(directory, EmptyCommittedSource, memTable, [segmentA], nextOffsetToApply: 2, flushThresholdBytes: 1);

                await using var enumerator = log.ReadFromAsync(0).GetAsyncEnumerator();

                // Pauses exactly after yielding segmentA's only entry — the segments snapshot for
                // this enumeration is already fixed at this point, but the MemTable has not been
                // read yet.
                (await enumerator.MoveNextAsync()).Should().BeTrue();
                enumerator.Current.Entry.LogicalOffset.Should().Be(0);
                enumerator.Current.Dispose();

                // Forces a real flush while the enumeration above is paused — moves entry 1 out
                // of the MemTable and into a new segment this enumeration never captured.
                log.TryPost(new WalRecordCommitted(SampleRecord(2, payload: "entry-2")));
                await WaitForSegmentCountAsync(directory, 2);

                var remaining = new List<ulong>();
                while (await enumerator.MoveNextAsync()) {
                    remaining.Add(enumerator.Current.Entry.LogicalOffset);
                    enumerator.Current.Dispose();
                }

                // Entry 1 existed (in the MemTable) for the entire duration of this
                // ReadFromAsync call and must never be skipped entirely. Whether entry 2 (appended
                // concurrently, while the enumeration was paused) also shows up is a genuine race —
                // MemTable is a mutable, shared object, so the actor may still be appending to the
                // very instance this call already captured, right up until the flush swaps it out.
                // That's not the invariant under test here; only entry 1's presence is.
                remaining.Should().Contain(1ul);
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
                await using (var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource, flushThresholdBytes: 1)) {
                    for (ulong i = 0; i < 3; i++)
                        log.TryPost(new WalRecordCommitted(SampleRecord(i)));

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
                    directory, EmptyCommittedSource, flushThresholdBytes: 1, compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null));

                const int count = 5;
                for (ulong i = 0; i < count; i++)
                    log.TryPost(new WalRecordCommitted(SampleRecord(i, payload: $"entry-{i}")));

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
                var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource, compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null));
                log.TryPost(new WalRecordCommitted(SampleRecord(0)));

                await log.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class Recovery {
        [Fact]
        public async Task Restarting_after_a_flush_still_finds_segment_backed_entries() {
            var directory = CreateTempDirectory();
            try {
                await using (var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource, flushThresholdBytes: 1)) {
                    for (ulong i = 0; i < 3; i++)
                        log.TryPost(new WalRecordCommitted(SampleRecord(i, payload: $"entry-{i}")));

                    await WaitForVisibleAsync(log, 2);
                }

                await using var reopened = await ShardLog.OpenAsync(directory, EmptyCommittedSource, flushThresholdBytes: 1);

                for (ulong offset = 0; offset < 3; offset++) {
                    reopened.TryGet(offset, out var handle).Should().BeTrue();
                    using (handle)
                        Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be($"entry-{offset}");
                }
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Restarting_skips_records_the_committed_source_redelivers_that_are_already_flushed() {
            var directory = CreateTempDirectory();
            try {
                await using (var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource, flushThresholdBytes: 1)) {
                    for (ulong i = 0; i < 3; i++)
                        log.TryPost(new WalRecordCommitted(SampleRecord(i, payload: $"entry-{i}")));

                    await WaitForVisibleAsync(log, 2);
                }

                Directory.GetFiles(directory, "*.sst").Should().HaveCount(3);

                // Simulates a restart where the committed-source (RaftReplica.ReadCommittedAsync)
                // replays its full history from the start, including offsets this ShardLog
                // already flushed in the previous session, followed by one genuinely new record.
                var redelivered = FixedCommittedSource(SampleRecord(0, "entry-0"), SampleRecord(1, "entry-1"), SampleRecord(2, "entry-2"), SampleRecord(3, "entry-3"));
                await using var reopened = await ShardLog.OpenAsync(directory, redelivered, flushThresholdBytes: 1);

                await WaitForVisibleAsync(reopened, 3);

                // Exactly one new segment — the redelivered offsets 0-2 must be skipped, not
                // re-flushed into duplicate segments. Polls rather than asserting immediately:
                // TryGet observing the new segment (Volatile.Write) and Directory.GetFiles
                // observing its renamed-from-.tmp file on disk are two independent signals: one
                // in-memory, one filesystem-level. Nothing here guarantees the second is
                // observable at the exact instant the first is (SstWriter.WriteAsync's own
                // create-temp-then-File.Move already runs before either becomes visible, but nothing
                // orders "visible to this test's next syscall" against it more tightly than that).
                await WaitForSegmentCountAsync(directory, 4);
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

                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);

                Directory.GetFiles(directory, "*.sst.merging").Should().BeEmpty();
                Directory.GetFiles(directory, "*.sst").Should().ContainSingle();

                for (ulong offset = 0; offset < 2; offset++) {
                    log.TryGet(offset, out var handle).Should().BeTrue();
                    using (handle)
                        Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be($"entry-{offset}");
                }

                // applying after recovery must continue from offset 2, not be filtered as stale
                log.TryPost(new WalRecordCommitted(SampleRecord(2)));
                await WaitForVisibleAsync(log, 2);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class PrepareSnapshotAsync {
        [Fact]
        public async Task Flushes_and_returns_the_segment_list_once_already_caught_up() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
                log.TryPost(new WalRecordCommitted(SampleRecord(0)));
                await WaitForVisibleAsync(log, 0);

                var segmentPaths = await log.PrepareSnapshotAsync(upToLogicalOffsetExclusive: 1);

                segmentPaths.Should().ContainSingle();
                Directory.GetFiles(directory, "*.sst").Should().HaveCount(1);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Does_not_flush_an_empty_MemTable() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);

                var segmentPaths = await log.PrepareSnapshotAsync(upToLogicalOffsetExclusive: 0);

                segmentPaths.Should().BeEmpty();
                Directory.GetFiles(directory, "*.sst").Should().BeEmpty();
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Waits_until_the_actor_catches_up_to_the_requested_offset() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);

                var prepareTask = log.PrepareSnapshotAsync(upToLogicalOffsetExclusive: 1);
                await Task.Delay(20);
                prepareTask.IsCompleted.Should().BeFalse("offset 0 hasn't been applied yet");

                log.TryPost(new WalRecordCommitted(SampleRecord(0)));

                var segmentPaths = await prepareTask.WaitAsync(TimeSpan.FromSeconds(5));
                segmentPaths.Should().ContainSingle();
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class InstallSnapshotAsync {
        [Fact]
        public async Task Replaces_segments_and_MemTable_wholesale() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
                log.TryPost(new WalRecordCommitted(SampleRecord(0, "stale-entry")));
                await WaitForVisibleAsync(log, 0);

                var snapshotDirectory = CreateTempDirectory();
                try {
                    var snapshotPath = Path.Combine(snapshotDirectory, $"segment-{100:D20}.sst");
                    await SstWriter.WriteAsync(snapshotPath, [
                        new LogEntry(100, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes("from-snapshot")),
                    ]);

                    await log.InstallSnapshotAsync([snapshotPath], nextOffsetToApply: 101);

                    log.TryGet(0, out _).Should().BeFalse("the stale entry must be gone");
                    log.TryGet(100, out var handle).Should().BeTrue();
                    using (handle)
                        Encoding.UTF8.GetString(handle.Entry.Payload.Span).Should().Be("from-snapshot");
                } finally {
                    Directory.Delete(snapshotDirectory, recursive: true);
                }
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task A_record_redelivered_below_the_new_offset_is_skipped() {
            var directory = CreateTempDirectory();
            try {
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource);

                var snapshotDirectory = CreateTempDirectory();
                try {
                    var snapshotPath = Path.Combine(snapshotDirectory, $"segment-{100:D20}.sst");
                    await SstWriter.WriteAsync(snapshotPath, [
                        new LogEntry(100, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes("from-snapshot")),
                    ]);
                    await log.InstallSnapshotAsync([snapshotPath], nextOffsetToApply: 101);

                    // A redelivered pre-snapshot record (e.g. a slow committed-source catching up
                    // on old history) must not be re-applied on top of the snapshot's own state.
                    log.TryPost(new WalRecordCommitted(SampleRecord(0, "stale-entry")));
                    await Task.Delay(20);

                    log.TryGet(0, out _).Should().BeFalse();
                } finally {
                    Directory.Delete(snapshotDirectory, recursive: true);
                }
            } finally {
                Directory.Delete(directory, recursive: true);
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
                await using var log = await ShardLog.OpenAsync(directory, EmptyCommittedSource, flushThresholdBytes: 60);

                const int count = 100;
                for (ulong i = 0; i < count; i++)
                    log.TryPost(new WalRecordCommitted(SampleRecord(i)));

                var stop = 0;
                var flickered = 0;

                var reader = Task.Run(() => {
                    var everVisible = new bool[count];
                    while (Volatile.Read(ref stop) == 0) {
                        for (var i = 0; i < count; i++) {
                            var found = log.TryGet((ulong)i, out var handle);
                            if (found)
                                handle.Dispose();

                            if (found)
                                everVisible[i] = true;
                            else if (everVisible[i])
                                Interlocked.Exchange(ref flickered, 1); // was visible, now isn't — the bug this guards against
                        }
                    }
                });

                await WaitForVisibleAsync(log, count - 1);
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

    static WalRecord SampleRecord(ulong offset, string payload = "hello world") =>
        new(WalRecordType.Append, RaftTerm: 0, RaftIndex: 0, offset, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes(payload));

    // ShardLog's own tests inject committed records directly via TryPost — this stands in for a
    // real committed-source (RaftReplica.ReadCommittedAsync) whenever a test has nothing to
    // redeliver on open/reopen.
    static async IAsyncEnumerable<WalRecord> EmptyCommittedSource([EnumeratorCancellation] CancellationToken cancellationToken) {
        await Task.CompletedTask;
        yield break;
    }

    // A committed-source stand-in that redelivers a fixed sequence — simulates a real
    // committed-source replaying its full history across a restart (RaftReplica does this on
    // every StartAsync, per its commitIndex never being persisted).
    static Func<CancellationToken, IAsyncEnumerable<WalRecord>> FixedCommittedSource(params WalRecord[] records) =>
        cancellationToken => YieldFixedAsync(records, cancellationToken);

    static async IAsyncEnumerable<WalRecord> YieldFixedAsync(WalRecord[] records, [EnumeratorCancellation] CancellationToken cancellationToken) {
        foreach (var record in records) {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return record;
        }
    }
}
