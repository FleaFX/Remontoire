using System.Text;
using FluentAssertions;

namespace Remontoire.Storage;

public class MemTableTests {
    public class Append {
        [Fact]
        public void Copies_the_entrys_data_so_the_original_source_can_be_reused_afterward() {
            using var table = new MemTable();
            var sourceBytes = "hello world"u8.ToArray();
            table.Append(SampleEntry(1, payload: sourceBytes));

            Array.Clear(sourceBytes); // simulate the original buffer being reused/returned to a pool

            table.TryGet(1, out var entry).Should().BeTrue();
            Encoding.UTF8.GetString(entry.Payload.Span).Should().Be("hello world");
        }
    }

    public class TryGet {
        [Fact]
        public void Returns_false_when_nothing_has_been_appended() {
            using var table = new MemTable();

            table.TryGet(0, out _).Should().BeFalse();
        }

        [Fact]
        public void Returns_false_for_an_offset_before_the_first_appended_entry() {
            using var table = new MemTable();
            table.Append(SampleEntry(10));

            table.TryGet(9, out _).Should().BeFalse();
        }

        [Fact]
        public void Returns_false_for_an_offset_not_yet_appended() {
            using var table = new MemTable();
            table.Append(SampleEntry(1));

            table.TryGet(2, out _).Should().BeFalse();
        }

        [Fact]
        public void Finds_entries_across_multiple_blocks() {
            using var table = new MemTable();
            const int count = 2500; // spans three 1024-capacity blocks

            for (ulong i = 0; i < count; i++)
                table.Append(SampleEntry(i));

            foreach (var offset in new ulong[] { 0, 1, 1023, 1024, 1025, 2047, 2048, count - 1 }) {
                table.TryGet(offset, out var entry).Should().BeTrue();
                entry.LogicalOffset.Should().Be(offset);
            }
        }

        [Fact]
        public void Starts_counting_offsets_from_the_first_appended_entry_not_from_zero() {
            using var table = new MemTable();
            table.Append(SampleEntry(1000));
            table.Append(SampleEntry(1001));

            table.TryGet(1000, out var first).Should().BeTrue();
            first.LogicalOffset.Should().Be(1000);

            table.TryGet(1001, out var second).Should().BeTrue();
            second.LogicalOffset.Should().Be(1001);
        }
    }

    public class FirstOffset {
        [Fact]
        public void Is_the_offset_of_the_first_appended_entry() {
            using var table = new MemTable();
            table.Append(SampleEntry(1000));
            table.Append(SampleEntry(1001));

            table.FirstOffset.Should().Be(1000);
        }
    }

    public class ScanFrom {
        [Fact]
        public void Yields_nothing_when_the_table_is_empty() {
            using var table = new MemTable();

            table.ScanFrom(0).Should().BeEmpty();
        }

        [Fact]
        public void Yields_entries_in_order_starting_from_the_requested_offset() {
            using var table = new MemTable();
            for (ulong i = 5; i < 10; i++)
                table.Append(SampleEntry(i));

            table.ScanFrom(7).Select(e => e.LogicalOffset).Should().Equal(7ul, 8ul, 9ul);
        }

        [Fact]
        public void Clamps_a_requested_offset_before_the_first_entry_to_the_first_entry() {
            using var table = new MemTable();
            for (ulong i = 5; i < 10; i++)
                table.Append(SampleEntry(i));

            table.ScanFrom(0).Select(e => e.LogicalOffset).Should().Equal(5ul, 6ul, 7ul, 8ul, 9ul);
        }

        [Fact]
        public void Yields_nothing_when_the_requested_offset_is_beyond_everything_appended() {
            using var table = new MemTable();
            table.Append(SampleEntry(1));

            table.ScanFrom(5).Should().BeEmpty();
        }

        [Fact]
        public void Spans_multiple_blocks_in_order() {
            using var table = new MemTable();
            const int count = 2500;

            for (ulong i = 0; i < count; i++)
                table.Append(SampleEntry(i));

            table.ScanFrom(1020).Select(e => e.LogicalOffset).Should().Equal(Enumerable.Range(1020, count - 1020).Select(i => (ulong)i));
        }

        [Fact]
        public void Snapshots_at_the_moment_of_the_call_not_at_the_moment_enumeration_starts() {
            using var table = new MemTable();
            table.Append(SampleEntry(1));

            var scan = table.ScanFrom(1); // snapshot should happen right here
            table.Append(SampleEntry(2)); // appended before enumeration begins

            scan.Select(e => e.LogicalOffset).Should().Equal(1ul); // offset 2 must NOT appear
        }

        [Fact]
        public void Does_not_include_entries_appended_after_enumeration_has_already_begun() {
            using var table = new MemTable();
            table.Append(SampleEntry(1));
            table.Append(SampleEntry(2));

            using var enumerator = table.ScanFrom(1).GetEnumerator();
            enumerator.MoveNext().Should().BeTrue();
            enumerator.Current.LogicalOffset.Should().Be(1);

            table.Append(SampleEntry(3)); // appended mid-enumeration

            enumerator.MoveNext().Should().BeTrue();
            enumerator.Current.LogicalOffset.Should().Be(2);
            enumerator.MoveNext().Should().BeFalse(); // offset 3 must NOT appear
        }
    }

    public class DisposeTests {
        [Fact]
        public void Does_not_throw_when_the_table_is_empty() {
            var table = new MemTable();

            var act = table.Dispose;

            act.Should().NotThrow();
        }

        [Fact]
        public void Does_not_throw_after_entries_have_been_appended() {
            var table = new MemTable();
            table.Append(SampleEntry(1));

            var act = table.Dispose;

            act.Should().NotThrow();
        }
    }

    static LogEntry SampleEntry(ulong logicalOffset, byte[]? payload = null) =>
        new(logicalOffset, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"),
            [new Header(Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"))],
            payload ?? Encoding.UTF8.GetBytes("hello world"));
}
