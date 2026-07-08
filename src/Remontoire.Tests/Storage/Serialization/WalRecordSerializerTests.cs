using System.Text;
using FluentAssertions;

namespace Remontoire.Storage.Serialization;

public class WalRecordSerializerTests {
    public class Write {
        [Fact]
        public void Throws_when_destination_too_small() {
            var record = SampleRecord();
            var buffer = new byte[WalRecordSerializer.GetEncodedLength(record) - 1];

            var act = () => WalRecordSerializer.Write(record, buffer);

            act.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void Returns_exactly_GetEncodedLength_bytes() {
            var record = SampleRecord();
            var buffer = new byte[WalRecordSerializer.GetEncodedLength(record) + 64]; // slack

            var written = WalRecordSerializer.Write(record, buffer);

            written.Should().Be(WalRecordSerializer.GetEncodedLength(record));
        }
    }

    public class TryRead {
        [Fact]
        public void Round_trips_a_record_with_headers_and_payload() {
            var original = SampleRecord();
            var encoded = Encode(original);

            using var result = WalRecordSerializer.TryRead(encoded);

            result.Status.Should().Be(WalRecordReadStatus.Success);
            result.BytesConsumed.Should().Be(encoded.Length);
            AssertEquivalent(result.Record, original);
        }

        [Fact]
        public void Round_trips_a_record_with_no_headers_empty_key_and_empty_payload() {
            var original = SampleRecord(partitionKey: "", headers: [], payload: "");
            var encoded = Encode(original);

            using var result = WalRecordSerializer.TryRead(encoded);

            result.Status.Should().Be(WalRecordReadStatus.Success);
            AssertEquivalent(result.Record, original);
        }

        [Fact]
        public void Reads_only_the_first_of_two_concatenated_records_and_reports_correct_bytesConsumed() {
            var first = SampleRecord(logicalOffset: 1);
            var second = SampleRecord(logicalOffset: 2);
            var buffer = Encode(first).Concat(Encode(second)).ToArray();

            using var result = WalRecordSerializer.TryRead(buffer);

            result.Status.Should().Be(WalRecordReadStatus.Success);
            result.Record.LogicalOffset.Should().Be(1);

            using var secondResult = WalRecordSerializer.TryRead(buffer.AsSpan(result.BytesConsumed));

            secondResult.Status.Should().Be(WalRecordReadStatus.Success);
            secondResult.Record.LogicalOffset.Should().Be(2);
        }

        [Fact]
        public void Reports_incomplete_when_fewer_than_5_bytes_are_available() {
            var encoded = Encode(SampleRecord());

            using var result = WalRecordSerializer.TryRead(encoded.AsSpan(0, 3));

            result.Status.Should().Be(WalRecordReadStatus.Incomplete);
        }

        [Theory]
        [InlineData(5)]  // right after the prefix — none of the body has arrived yet
        [InlineData(20)] // truncated partway through the fixed body header
        public void Reports_incomplete_when_truncated_partway_through_the_body(int truncatedLength) {
            var encoded = Encode(SampleRecord());

            using var result = WalRecordSerializer.TryRead(encoded.AsSpan(0, truncatedLength));

            result.Status.Should().Be(WalRecordReadStatus.Incomplete);
        }

        [Fact]
        public void Reports_corrupt_when_a_body_byte_is_flipped() {
            var encoded = Encode(SampleRecord());
            encoded[^1] ^= 0xFF;

            using var result = WalRecordSerializer.TryRead(encoded);

            result.Status.Should().Be(WalRecordReadStatus.Corrupt);
        }

        [Fact]
        public void Reports_corrupt_when_the_version_byte_is_unknown() {
            var encoded = Encode(SampleRecord());
            encoded[0] = 0xFF;

            using var result = WalRecordSerializer.TryRead(encoded);

            result.Status.Should().Be(WalRecordReadStatus.Corrupt);
        }

        [Fact]
        public void Copies_data_so_the_original_source_can_be_reused_afterward() {
            var encoded = Encode(SampleRecord(payload: "hello world"));

            using var result = WalRecordSerializer.TryRead(encoded);
            Array.Clear(encoded); // simulate the original buffer being reused/returned to a pool

            Encoding.UTF8.GetString(result.Record.Payload.Span).Should().Be("hello world");
        }

        [Fact]
        public void Disposing_the_default_result_does_not_throw() {
            var result = default(WalReadResult);

            var act = result.Dispose;

            act.Should().NotThrow();
        }

        [Fact]
        public void Disposing_twice_does_not_throw() {
            var result = WalRecordSerializer.TryRead(Encode(SampleRecord()));
            result.Dispose();

            var act = result.Dispose;

            act.Should().NotThrow();
        }
    }

    static WalRecord SampleRecord(
        WalRecordType type = WalRecordType.Append,
        ulong raftTerm = 7,
        ulong raftIndex = 42,
        ulong logicalOffset = 123,
        ulong timestampMicros = 999,
        string partitionKey = "order-42",
        IReadOnlyList<Header>? headers = null,
        string payload = "hello world") {
        headers ??= [
            new Header(Encoding.UTF8.GetBytes("correlation-id"), Encoding.UTF8.GetBytes("abc-123")),
            new Header(Encoding.UTF8.GetBytes("event-type"), Encoding.UTF8.GetBytes("OrderPlaced")),
        ];

        return new WalRecord(type, raftTerm, raftIndex, logicalOffset, timestampMicros,
            Encoding.UTF8.GetBytes(partitionKey), headers, Encoding.UTF8.GetBytes(payload));
    }

    static byte[] Encode(in WalRecord record) {
        var buffer = new byte[WalRecordSerializer.GetEncodedLength(record)];
        WalRecordSerializer.Write(record, buffer).Should().Be(buffer.Length);
        return buffer;
    }

    static void AssertEquivalent(in WalRecord actual, in WalRecord expected) {
        actual.RecordType.Should().Be(expected.RecordType);
        actual.RaftTerm.Should().Be(expected.RaftTerm);
        actual.RaftIndex.Should().Be(expected.RaftIndex);
        actual.LogicalOffset.Should().Be(expected.LogicalOffset);
        actual.TimestampMicros.Should().Be(expected.TimestampMicros);
        actual.PartitionKey.ToArray().Should().Equal(expected.PartitionKey.ToArray());
        actual.Payload.ToArray().Should().Equal(expected.Payload.ToArray());

        actual.Headers.Count.Should().Be(expected.Headers.Count);
        for (var i = 0; i < expected.Headers.Count; i++) {
            actual.Headers[i].Key.ToArray().Should().Equal(expected.Headers[i].Key.ToArray());
            actual.Headers[i].Value.ToArray().Should().Equal(expected.Headers[i].Value.ToArray());
        }
    }
}
