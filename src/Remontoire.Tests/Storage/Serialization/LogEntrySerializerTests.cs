using System.Text;
using FluentAssertions;

namespace Remontoire.Storage.Serialization;

public class LogEntrySerializerTests {
    public class Write {
        [Fact]
        public void Throws_when_destination_too_small() {
            var entry = SampleEntry();
            var buffer = new byte[LogEntrySerializer.GetEncodedLength(entry) - 1];

            var act = () => LogEntrySerializer.Write(entry, buffer);

            act.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void Returns_exactly_GetEncodedLength_bytes() {
            var entry = SampleEntry();
            var buffer = new byte[LogEntrySerializer.GetEncodedLength(entry) + 64]; // slack

            var written = LogEntrySerializer.Write(entry, buffer);

            written.Should().Be(LogEntrySerializer.GetEncodedLength(entry));
        }
    }

    public class TryRead {
        [Fact]
        public void Round_trips_an_entry_with_headers_and_payload() {
            var original = SampleEntry();
            var encoded = Encode(original);

            using var result = LogEntrySerializer.TryRead(encoded);

            result.Status.Should().Be(LogEntryReadStatus.Success);
            result.BytesConsumed.Should().Be(encoded.Length);
            AssertEquivalent(result.Entry, original);
        }

        [Fact]
        public void Round_trips_an_entry_with_no_headers_empty_key_and_empty_payload() {
            var original = SampleEntry(partitionKey: "", headers: [], payload: "");
            var encoded = Encode(original);

            using var result = LogEntrySerializer.TryRead(encoded);

            result.Status.Should().Be(LogEntryReadStatus.Success);
            AssertEquivalent(result.Entry, original);
        }

        [Fact]
        public void Reads_only_the_first_of_two_concatenated_entries_and_reports_correct_bytesConsumed() {
            var first = SampleEntry(logicalOffset: 1);
            var second = SampleEntry(logicalOffset: 2);
            var buffer = Encode(first).Concat(Encode(second)).ToArray();

            using var result = LogEntrySerializer.TryRead(buffer);

            result.Status.Should().Be(LogEntryReadStatus.Success);
            result.BytesConsumed.Should().Be(Encode(first).Length);
            result.Entry.LogicalOffset.Should().Be(1);
        }

        [Fact]
        public void Returns_incomplete_when_fewer_bytes_than_the_prefix_are_available() {
            using var result = LogEntrySerializer.TryRead(new byte[3]);

            result.Status.Should().Be(LogEntryReadStatus.Incomplete);
        }

        [Fact]
        public void Returns_incomplete_when_the_body_is_truncated() {
            var encoded = Encode(SampleEntry());

            using var result = LogEntrySerializer.TryRead(encoded.AsSpan(0, encoded.Length - 1));

            result.Status.Should().Be(LogEntryReadStatus.Incomplete);
        }

        [Fact]
        public void Returns_corrupt_when_the_checksum_does_not_match() {
            var encoded = Encode(SampleEntry());
            encoded[^1] ^= 0xFF;

            using var result = LogEntrySerializer.TryRead(encoded);

            result.Status.Should().Be(LogEntryReadStatus.Corrupt);
        }

        [Fact]
        public void Copies_data_so_the_original_source_can_be_reused_afterward() {
            var sourceBytes = "hello world"u8.ToArray();
            var encoded = Encode(SampleEntry(payload: Encoding.UTF8.GetString(sourceBytes)));

            using var result = LogEntrySerializer.TryRead(encoded);
            Array.Clear(encoded); // simulate the original buffer being reused/returned to a pool

            Encoding.UTF8.GetString(result.Entry.Payload.Span).Should().Be("hello world");
        }
    }

    static LogEntry SampleEntry(
        ulong logicalOffset = 123,
        ulong timestampMicros = 999,
        string partitionKey = "order-42",
        IReadOnlyList<WalHeader>? headers = null,
        string payload = "hello world") {
        headers ??= [
            new WalHeader(Encoding.UTF8.GetBytes("correlation-id"), Encoding.UTF8.GetBytes("abc-123")),
            new WalHeader(Encoding.UTF8.GetBytes("event-type"), Encoding.UTF8.GetBytes("OrderPlaced")),
        ];

        return new LogEntry(logicalOffset, timestampMicros,
            Encoding.UTF8.GetBytes(partitionKey), headers, Encoding.UTF8.GetBytes(payload));
    }

    static byte[] Encode(in LogEntry entry) {
        var buffer = new byte[LogEntrySerializer.GetEncodedLength(entry)];
        LogEntrySerializer.Write(entry, buffer).Should().Be(buffer.Length);
        return buffer;
    }

    static void AssertEquivalent(in LogEntry actual, in LogEntry expected) {
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
