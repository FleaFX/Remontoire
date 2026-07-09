using FluentAssertions;
using Remontoire.Storage;
using Remontoire.Storage.Serialization;

namespace Remontoire.Raft;

public class WalRaftLogTests {
    [Fact]
    public async Task Starts_empty_at_the_snapshot_base() {
        var directory = TempWalDirectory();
        try {
            await using var log = await WalRaftLog.OpenAsync(directory);

            log.LastIndex.Should().Be(0);
            log.LastTerm.Should().Be(0);
            (await ToListAsync(log.ReadFromAsync(1))).Should().BeEmpty();
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task AppendAsync_then_ReadFromAsync_yields_the_appended_entries_in_order() {
        var directory = TempWalDirectory();
        try {
            await using var log = await WalRaftLog.OpenAsync(directory);

            await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2), Entry(term: 1, index: 3)]);

            log.LastIndex.Should().Be(3);
            log.LastTerm.Should().Be(1);
            (await ToListAsync(log.ReadFromAsync(2))).Select(r => r.RaftIndex).Should().Equal(2ul, 3ul);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task GetTermAtAsync_returns_the_term_of_the_entry_at_that_index() {
        var directory = TempWalDirectory();
        try {
            await using var log = await WalRaftLog.OpenAsync(directory);

            await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 2, index: 2)]);

            (await log.GetTermAtAsync(1)).Should().Be(1);
            (await log.GetTermAtAsync(2)).Should().Be(2);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task TruncateFromAsync_removes_the_entry_and_everything_after_it() {
        var directory = TempWalDirectory();
        try {
            await using var log = await WalRaftLog.OpenAsync(directory);

            await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2), Entry(term: 2, index: 3)]);
            await log.TruncateFromAsync(2);

            log.LastIndex.Should().Be(1);
            log.LastTerm.Should().Be(1);
            (await ToListAsync(log.ReadFromAsync(1))).Select(r => r.RaftIndex).Should().Equal(1ul);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task TruncateFromAsync_lets_a_later_append_reuse_the_freed_index() {
        var directory = TempWalDirectory();
        try {
            await using var log = await WalRaftLog.OpenAsync(directory);

            await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2)]);
            await log.TruncateFromAsync(2);
            await log.AppendAsync([Entry(term: 2, index: 2)]);

            log.LastIndex.Should().Be(2);
            log.LastTerm.Should().Be(2);
            (await log.GetTermAtAsync(2)).Should().Be(2);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task OpenAsync_recovers_the_position_index_from_an_existing_directory() {
        var directory = TempWalDirectory();
        try {
            await using (var log = await WalRaftLog.OpenAsync(directory))
                await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2), Entry(term: 2, index: 3)]);

            await using var reopened = await WalRaftLog.OpenAsync(directory);

            reopened.LastIndex.Should().Be(3);
            reopened.LastTerm.Should().Be(2);
            (await reopened.GetTermAtAsync(2)).Should().Be(1);
            (await ToListAsync(reopened.ReadFromAsync(2))).Select(r => r.RaftIndex).Should().Equal(2ul, 3ul);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task OpenAsync_never_recovers_a_truncated_away_suffix() {
        var directory = TempWalDirectory();
        try {
            await using (var log = await WalRaftLog.OpenAsync(directory)) {
                await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2)]);
                await log.TruncateFromAsync(2);
            }

            await using var reopened = await WalRaftLog.OpenAsync(directory);

            reopened.LastIndex.Should().Be(1);
            (await ToListAsync(reopened.ReadFromAsync(1))).Select(r => r.RaftIndex).Should().Equal(1ul);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    public class Rotation {
        [Fact]
        public async Task Rotates_to_a_new_file_once_the_active_one_exceeds_the_threshold() {
            var directory = TempWalDirectory();
            try {
                await using var log = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1);

                // The active file starts empty; appending the first entry immediately pushes it
                // past the threshold, so this single append leaves one sealed file behind plus
                // the fresh (still-empty) active one.
                await log.AppendAsync([Entry(term: 1, index: 1)]);

                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(2);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Reads_and_recovers_correctly_across_a_rotation_boundary() {
            var directory = TempWalDirectory();
            try {
                await using (var log = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1)) {
                    await log.AppendAsync([Entry(term: 1, index: 1)]);
                    await log.AppendAsync([Entry(term: 1, index: 2)]);
                    await log.AppendAsync([Entry(term: 2, index: 3)]);

                    (await ToListAsync(log.ReadFromAsync(1))).Select(r => r.RaftIndex).Should().Equal(1ul, 2ul, 3ul);
                    (await log.GetTermAtAsync(2)).Should().Be(1);
                }

                await using var reopened = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1);

                reopened.LastIndex.Should().Be(3);
                reopened.LastTerm.Should().Be(2);
                (await ToListAsync(reopened.ReadFromAsync(1))).Select(r => r.RaftIndex).Should().Equal(1ul, 2ul, 3ul);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task TruncateFromAsync_reaches_into_an_already_sealed_file_and_deletes_everything_after_it() {
            var directory = TempWalDirectory();
            try {
                await using var log = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1);

                await log.AppendAsync([Entry(term: 1, index: 1)]); // seals into its own file
                await log.AppendAsync([Entry(term: 1, index: 2)]); // seals into its own file
                await log.AppendAsync([Entry(term: 1, index: 3)]); // seals into its own file too — leaves a fresh, empty active file

                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(4);

                await log.TruncateFromAsync(2); // reaches into the (sealed) file holding index 2

                log.LastIndex.Should().Be(1);
                log.LastTerm.Should().Be(1);
                // index 1's file survives untouched; index 2's file survives too, truncated back
                // to empty and reopened as the active file — the two files after it are deleted.
                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(2);
                (await ToListAsync(log.ReadFromAsync(1))).Select(r => r.RaftIndex).Should().Equal(1ul);

                // the truncated, reopened file must still accept new appends correctly
                await log.AppendAsync([Entry(term: 2, index: 2)]);
                log.LastIndex.Should().Be(2);
                (await log.GetTermAtAsync(2)).Should().Be(2);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class CompactToAsync {
        [Fact]
        public async Task Deletes_sealed_files_entirely_covered_by_the_new_snapshot_base() {
            var directory = TempWalDirectory();
            try {
                await using var log = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1);

                await log.AppendAsync([Entry(term: 1, index: 1)]);
                await log.AppendAsync([Entry(term: 1, index: 2)]);
                await log.AppendAsync([Entry(term: 1, index: 3)]);

                await log.CompactToAsync(lastIncludedIndex: 2, lastIncludedTerm: 1);

                log.SnapshotIndex.Should().Be(2);
                log.SnapshotTerm.Should().Be(1);
                // index 1's and 2's files are entirely covered and deleted; index 3's sealed
                // file and the fresh, empty active file behind it are not.
                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(2);
                (await ToListAsync(log.ReadFromAsync(3))).Select(r => r.RaftIndex).Should().Equal(3ul);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Leaves_a_sealed_file_that_straddles_the_boundary_untouched() {
            var directory = TempWalDirectory();
            try {
                var entry1 = Entry(term: 1, index: 1);
                var threshold = WalRecordSerializer.GetEncodedLength(entry1) + 1; // room for entry 1 alone, not entry 1+2 together
                await using var log = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: threshold);

                await log.AppendAsync([entry1, Entry(term: 1, index: 2)]); // both land in the same file, which then rotates
                await log.AppendAsync([Entry(term: 1, index: 3)]); // active file

                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(2);

                await log.CompactToAsync(lastIncludedIndex: 1, lastIncludedTerm: 1); // inside the sealed file's [1,2] range

                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(2); // untouched — straddles the boundary
                (await log.GetTermAtAsync(2)).Should().Be(1); // still readable
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task OpenAsync_finishes_an_interrupted_compaction_by_deleting_the_covered_file_without_scanning_it() {
            var directory = TempWalDirectory();
            try {
                await using (var log = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1)) {
                    await log.AppendAsync([Entry(term: 1, index: 1)]);
                    await log.AppendAsync([Entry(term: 1, index: 2)]);
                }

                // Simulates a crash between CompactToAsync's marker save and its file deletion:
                // write the marker directly, leave the now-covered file behind. Three files exist
                // at this point: index 1's (now fully covered), index 2's, and the empty active one.
                await SnapshotMarker.SaveAsync(directory, 1, 1);
                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(3);

                await using var reopened = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1);

                reopened.SnapshotIndex.Should().Be(1);
                reopened.SnapshotTerm.Should().Be(1);
                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(2);
                reopened.LastIndex.Should().Be(2);
                (await ToListAsync(reopened.ReadFromAsync(2))).Select(r => r.RaftIndex).Should().Equal(2ul);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class InstallSnapshotAsync {
        [Fact]
        public async Task Discards_all_existing_content_and_resets_the_base() {
            var directory = TempWalDirectory();
            try {
                await using var log = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1);
                await log.AppendAsync([Entry(term: 1, index: 1)]);
                await log.AppendAsync([Entry(term: 1, index: 2)]);

                await log.InstallSnapshotAsync(lastIncludedIndex: 100, lastIncludedTerm: 5);

                log.SnapshotIndex.Should().Be(100);
                log.SnapshotTerm.Should().Be(5);
                log.LastIndex.Should().Be(100);
                log.LastTerm.Should().Be(5);
                (await ToListAsync(log.ReadFromAsync(101))).Should().BeEmpty(); // nothing beyond the new, empty base

                await log.AppendAsync([Entry(term: 5, index: 101)]);
                log.LastIndex.Should().Be(101);
                (await log.GetTermAtAsync(101)).Should().Be(5);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task OpenAsync_recovers_correctly_after_installing_a_snapshot() {
            var directory = TempWalDirectory();
            try {
                await using (var log = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1)) {
                    await log.AppendAsync([Entry(term: 1, index: 1)]);
                    await log.AppendAsync([Entry(term: 1, index: 2)]);
                    await log.InstallSnapshotAsync(lastIncludedIndex: 100, lastIncludedTerm: 5);
                    await log.AppendAsync([Entry(term: 5, index: 101)]);
                }

                // Pre-snapshot files are gone; index 101's own append immediately rotates (threshold
                // 1) into its own sealed file plus a fresh, empty active one behind it.
                Directory.GetFiles(directory, "wal-*.log").Should().HaveCount(2);

                await using var reopened = await WalRaftLog.OpenAsync(directory, rotationThresholdBytes: 1);

                reopened.SnapshotIndex.Should().Be(100);
                reopened.SnapshotTerm.Should().Be(5);
                reopened.LastIndex.Should().Be(101);
                (await ToListAsync(reopened.ReadFromAsync(101))).Select(r => r.RaftIndex).Should().Equal(101ul);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    static string TempWalDirectory() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }

    static WalRecord Entry(ulong term, ulong index) =>
        new(WalRecordType.Append, RaftTerm: term, RaftIndex: index, LogicalOffset: index - 1, TimestampMicros: 42,
            PartitionKey: "order-42"u8.ToArray(), Headers: [], Payload: "hello world"u8.ToArray());

    static async Task<List<WalRecord>> ToListAsync(IAsyncEnumerable<WalRecord> records) {
        var list = new List<WalRecord>();
        await foreach (var record in records)
            list.Add(record);
        return list;
    }
}
