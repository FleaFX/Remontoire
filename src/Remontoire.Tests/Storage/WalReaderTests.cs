using System.Text;
using FluentAssertions;
using Remontoire.Storage.Serialization;

namespace Remontoire.Storage;

public class WalReaderTests {
    public class ReadFromAsync {
        [Fact]
        public async Task Reads_back_records_written_by_WalWriter_in_order() {
            var path = Path.GetTempFileName();
            try {
                var writer = await WalWriter.OpenAsync(path);
                await writer.AppendAsync(SampleRecord(1));
                await writer.AppendAsync(SampleRecord(2));
                await writer.AppendAsync(SampleRecord(3));
                await writer.DisposeAsync();

                var offsets = await ReadAllOffsetsAsync(path);

                offsets.Should().Equal(1ul, 2ul, 3ul);
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Reads_starting_from_a_given_byte_position() {
            var path = Path.GetTempFileName();
            try {
                var first = Encode(SampleRecord(1));
                var second = Encode(SampleRecord(2));
                await File.WriteAllBytesAsync(path, [.. first, .. second]);

                var offsets = await ReadAllOffsetsAsync(path, startPosition: first.Length);

                offsets.Should().Equal(2ul);
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Reading_an_empty_file_yields_nothing() {
            var path = Path.GetTempFileName();
            try {
                var offsets = await ReadAllOffsetsAsync(path);

                offsets.Should().BeEmpty();
            } finally {
                File.Delete(path);
            }
        }

        [Theory]
        [MemberData(nameof(AllTruncationPoints))]
        public async Task Stops_cleanly_at_a_torn_write_regardless_of_truncation_point(int truncatedLength) {
            var path = Path.GetTempFileName();
            try {
                var firstLength = Encode(SampleRecord(1)).Length;
                var full = (byte[])[.. Encode(SampleRecord(1)), .. Encode(SampleRecord(2))];

                await File.WriteAllBytesAsync(path, full.AsSpan(0, truncatedLength).ToArray());

                var offsets = await ReadAllOffsetsAsync(path);

                if (truncatedLength < firstLength)
                    offsets.Should().BeEmpty();
                else if (truncatedLength < full.Length)
                    offsets.Should().Equal(1ul);
                else
                    offsets.Should().Equal(1ul, 2ul);
            } finally {
                File.Delete(path);
            }
        }

        public static IEnumerable<object[]> AllTruncationPoints() {
            var totalLength = Encode(SampleRecord(1)).Length + Encode(SampleRecord(2)).Length;

            for (var i = 0; i <= totalLength; i++)
                yield return [i];
        }

        [Fact]
        public async Task Stops_at_a_corrupted_record_and_does_not_yield_records_after_it() {
            var path = Path.GetTempFileName();
            try {
                var first = Encode(SampleRecord(1));
                var second = Encode(SampleRecord(2));
                second[^1] ^= 0xFF; // corrupt the second record's payload
                var third = Encode(SampleRecord(3));

                await File.WriteAllBytesAsync(path, [.. first, .. second, .. third]);

                var offsets = await ReadAllOffsetsAsync(path);

                offsets.Should().Equal(1ul);
            } finally {
                File.Delete(path);
            }
        }
    }

    static async Task<List<ulong>> ReadAllOffsetsAsync(string path, long startPosition = 0) {
        var reader = new WalReader(path);
        var offsets = new List<ulong>();

        await foreach (var result in reader.ReadFromAsync(startPosition)) {
            using (result)
                offsets.Add(result.Record.LogicalOffset);
        }

        return offsets;
    }

    static WalRecord SampleRecord(ulong logicalOffset) =>
        new(WalRecordType.Append, RaftTerm: 0, RaftIndex: 0, logicalOffset, TimestampMicros: 42,
            PartitionKey: Encoding.UTF8.GetBytes("order-42"), Headers: [], Payload: Encoding.UTF8.GetBytes("hello world"));

    static byte[] Encode(in WalRecord record) {
        var buffer = new byte[WalRecordSerializer.GetEncodedLength(record)];
        WalRecordSerializer.Write(record, buffer);
        return buffer;
    }
}
