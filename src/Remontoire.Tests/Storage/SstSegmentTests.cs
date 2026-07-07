using System.Text;
using FluentAssertions;

namespace Remontoire.Storage;

public class SstSegmentTests {
    public class OpenAsync {
        [Fact]
        public async Task Exposes_MinOffset_and_MaxOffset() {
            var path = await WriteSegmentAsync(SampleEntries(5, startOffset: 10));
            try {
                using var segment = await SstSegment.OpenAsync(path);

                segment.MinOffset.Should().Be(10);
                segment.MaxOffset.Should().Be(14);
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Throws_when_the_file_has_no_valid_magic() {
            var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            await File.WriteAllBytesAsync(path, new byte[64]);
            try {
                var act = async () => await SstSegment.OpenAsync(path);

                await act.Should().ThrowAsync<InvalidDataException>();
            } finally {
                File.Delete(path);
            }
        }
    }

    public class TryGet {
        [Fact]
        public async Task Finds_entries_across_multiple_sparse_index_blocks() {
            const int count = 50;
            var path = await WriteSegmentAsync(SampleEntries(count), indexIntervalRecords: 4);
            try {
                using var segment = await SstSegment.OpenAsync(path);

                foreach (var offset in new ulong[] { 0, 1, 3, 4, 5, 23, 24, 49 }) {
                    segment.TryGet(offset, out var result).Should().BeTrue();
                    using (result)
                        result.Entry.LogicalOffset.Should().Be(offset);
                }
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Returns_false_for_an_offset_below_MinOffset() {
            var path = await WriteSegmentAsync(SampleEntries(5, startOffset: 10));
            try {
                using var segment = await SstSegment.OpenAsync(path);

                segment.TryGet(9, out _).Should().BeFalse();
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Returns_false_for_an_offset_above_MaxOffset() {
            var path = await WriteSegmentAsync(SampleEntries(5, startOffset: 10));
            try {
                using var segment = await SstSegment.OpenAsync(path);

                segment.TryGet(15, out _).Should().BeFalse();
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Copies_data_so_it_survives_the_segments_lifetime() {
            var path = await WriteSegmentAsync(SampleEntries(1));
            try {
                using var segment = await SstSegment.OpenAsync(path);

                segment.TryGet(0, out var result).Should().BeTrue();
                using (result)
                    Encoding.UTF8.GetString(result.Entry.Payload.Span).Should().Be("hello world-0");
            } finally {
                File.Delete(path);
            }
        }
    }

    public class ScanFrom {
        [Fact]
        public async Task Yields_entries_in_order_starting_from_the_requested_offset() {
            const int count = 30;
            var path = await WriteSegmentAsync(SampleEntries(count), indexIntervalRecords: 4);
            try {
                using var segment = await SstSegment.OpenAsync(path);

                var offsets = new List<ulong>();
                foreach (var result in segment.ScanFrom(23))
                    using (result)
                        offsets.Add(result.Entry.LogicalOffset);

                offsets.Should().Equal(Enumerable.Range(23, count - 23).Select(i => (ulong)i));
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Yields_nothing_when_the_requested_offset_is_beyond_MaxOffset() {
            var path = await WriteSegmentAsync(SampleEntries(5));
            try {
                using var segment = await SstSegment.OpenAsync(path);

                segment.ScanFrom(100).Should().BeEmpty();
            } finally {
                File.Delete(path);
            }
        }
    }

    static async Task<string> WriteSegmentAsync(IEnumerable<LogEntry> entries, int indexIntervalRecords = Sst.DefaultSparseInterval) {
        var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        await SstWriter.WriteAsync(path, entries, indexIntervalRecords);
        return path;
    }

    static IEnumerable<LogEntry> SampleEntries(int count, ulong startOffset = 0) {
        for (var i = 0; i < count; i++) {
            var offset = startOffset + (ulong)i;
            yield return new LogEntry(offset, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [],
                Encoding.UTF8.GetBytes($"hello world-{offset}"));
        }
    }
}
