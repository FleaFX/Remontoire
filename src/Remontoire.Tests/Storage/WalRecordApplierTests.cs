using System.Text;
using FluentAssertions;

namespace Remontoire.Storage;

public class WalRecordApplierTests {
    public class Apply {
        [Fact]
        public void Appends_a_LogEntry_to_the_target_for_an_Append_record() {
            using var table = new MemTable();
            var applier = new WalRecordApplier(table);
            var record = SampleRecord(WalRecordType.Append, logicalOffset: 42);

            applier.Apply(record);

            table.TryGet(42, out var entry).Should().BeTrue();
            entry.LogicalOffset.Should().Be(42);
            entry.TimestampMicros.Should().Be(record.TimestampMicros);
            entry.PartitionKey.ToArray().Should().Equal(record.PartitionKey.ToArray());
            entry.Payload.ToArray().Should().Equal(record.Payload.ToArray());
            entry.Headers.Count.Should().Be(record.Headers.Count);
        }

        [Fact]
        public void Is_a_no_op_for_an_Ack_record() =>
            AssertNoOp(WalRecordType.Ack);

        [Fact]
        public void Is_a_no_op_for_a_ShardConfigChange_record() =>
            AssertNoOp(WalRecordType.ShardConfigChange);

        [Fact]
        public void Is_a_no_op_for_a_NoOp_record() =>
            AssertNoOp(WalRecordType.NoOp);

        static void AssertNoOp(WalRecordType type) {
            using var table = new MemTable();
            var applier = new WalRecordApplier(table);

            applier.Apply(SampleRecord(type, logicalOffset: 1));

            table.TryGet(1, out _).Should().BeFalse();
        }
    }

    static WalRecord SampleRecord(WalRecordType type, ulong logicalOffset) =>
        new(type, RaftTerm: 0, RaftIndex: 0, logicalOffset, TimestampMicros: 42,
            Encoding.UTF8.GetBytes("order-42"),
            [new WalHeader(Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"))],
            Encoding.UTF8.GetBytes("hello world"));
}
