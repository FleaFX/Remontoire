using System.Text;
using FluentAssertions;

namespace Remontoire.Storage;

public class CompactorTests {
    public class RunAsync {
        [Fact]
        public async Task Merges_all_segments_into_one_when_no_size_limit_is_set() {
            var directory = CreateTempDirectory();
            try {
                await WriteSegmentAsync(directory, SampleEntries(0, 5));
                await WriteSegmentAsync(directory, SampleEntries(5, 5));
                await WriteSegmentAsync(directory, SampleEntries(10, 5));

                await Compactor.RunAsync(directory, new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null));

                var files = Directory.GetFiles(directory, "*.sst");
                files.Should().HaveCount(1);

                await AssertSegmentContainsAsync(files[0], Enumerable.Range(0, 15).Select(i => (ulong)i));
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Leaves_a_single_segment_untouched() {
            var directory = CreateTempDirectory();
            try {
                var path = await WriteSegmentAsync(directory, SampleEntries(0, 5));

                await Compactor.RunAsync(directory, new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null));

                Directory.GetFiles(directory, "*.sst").Should().Equal(path);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Does_not_merge_segments_newer_than_MaxAge() {
            var directory = CreateTempDirectory();
            try {
                var first = await WriteSegmentAsync(directory, SampleEntries(0, 5));
                var second = await WriteSegmentAsync(directory, SampleEntries(5, 5));

                await Compactor.RunAsync(directory, new CompactionPolicy(MaxAge: TimeSpan.FromDays(1), MaxMergedSegmentBytes: null));

                Directory.GetFiles(directory, "*.sst").Should().BeEquivalentTo([first, second]);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Packs_segments_into_multiple_groups_when_MaxMergedSegmentBytes_is_exceeded() {
            var directory = CreateTempDirectory();
            try {
                var firstPath = await WriteSegmentAsync(directory, SampleEntries(0, 5));
                await WriteSegmentAsync(directory, SampleEntries(5, 5));
                await WriteSegmentAsync(directory, SampleEntries(10, 5));

                // A limit smaller than even one segment's own size forces every segment into
                // its own group — nothing ever fits together, so nothing gets merged.
                var tinyLimit = new FileInfo(firstPath).Length - 1;

                await Compactor.RunAsync(directory, new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: tinyLimit));

                Directory.GetFiles(directory, "*.sst").Should().HaveCount(3);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Merged_segment_preserves_offset_order_across_originals() {
            var directory = CreateTempDirectory();
            try {
                await WriteSegmentAsync(directory, SampleEntries(0, 3));
                await WriteSegmentAsync(directory, SampleEntries(3, 3));

                await Compactor.RunAsync(directory, new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null));

                var files = Directory.GetFiles(directory, "*.sst");
                using var segment = await SstSegment.OpenAsync(files[0]);
                segment.MinOffset.Should().Be(0);
                segment.MaxOffset.Should().Be(5);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    public class PruneAckedSegmentsAsync {
        [Fact]
        public async Task Deletes_a_segment_fully_covered_by_the_watermark() {
            var directory = CreateTempDirectory();
            try {
                var path = await WriteSegmentAsync(directory, SampleEntries(0, 5)); // offsets 0..4
                var policy = new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => ValueTask.FromResult(4UL));

                await Compactor.PruneAckedSegmentsAsync(directory, policy);

                File.Exists(path).Should().BeFalse();
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Leaves_a_segment_that_straddles_the_watermark_untouched() {
            var directory = CreateTempDirectory();
            try {
                var path = await WriteSegmentAsync(directory, SampleEntries(0, 5)); // offsets 0..4
                var policy = new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => ValueTask.FromResult(2UL));

                await Compactor.PruneAckedSegmentsAsync(directory, policy);

                File.Exists(path).Should().BeTrue("offset 3 and 4 in this segment are not yet covered by the watermark");
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Deletes_only_the_segments_fully_covered_among_several() {
            var directory = CreateTempDirectory();
            try {
                var covered = await WriteSegmentAsync(directory, SampleEntries(0, 5)); // offsets 0..4
                var straddling = await WriteSegmentAsync(directory, SampleEntries(5, 5)); // offsets 5..9
                var policy = new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => ValueTask.FromResult(4UL));

                await Compactor.PruneAckedSegmentsAsync(directory, policy);

                File.Exists(covered).Should().BeFalse();
                File.Exists(straddling).Should().BeTrue();
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Does_nothing_when_the_policy_has_no_watermark_delegate() {
            var directory = CreateTempDirectory();
            try {
                var path = await WriteSegmentAsync(directory, SampleEntries(0, 5));
                var policy = new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null);

                await Compactor.PruneAckedSegmentsAsync(directory, policy);

                File.Exists(path).Should().BeTrue();
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

    static async Task<string> WriteSegmentAsync(string directory, IEnumerable<LogEntry> entries) {
        var materialized = entries.ToList();
        var path = Path.Combine(directory, $"segment-{materialized[0].LogicalOffset:D20}.sst");
        await SstWriter.WriteAsync(path, materialized);
        return path;
    }

    static async Task AssertSegmentContainsAsync(string path, IEnumerable<ulong> expectedOffsets) {
        using var segment = await SstSegment.OpenAsync(path);
        var offsets = new List<ulong>();

        foreach (var result in segment.ScanFrom(segment.MinOffset))
            using (result)
                offsets.Add(result.Entry.LogicalOffset);

        offsets.Should().Equal(expectedOffsets);
    }

    static IEnumerable<LogEntry> SampleEntries(ulong startOffset, int count) {
        for (var i = 0; i < count; i++) {
            var offset = startOffset + (ulong)i;
            yield return new LogEntry(offset, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [],
                Encoding.UTF8.GetBytes($"hello world-{offset}"));
        }
    }
}
