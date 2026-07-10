using FluentAssertions;

namespace Remontoire.Sharding;

public class MetaLogRecordTests {
    public class EncodeDecode {
        [Fact]
        public void Round_trips_a_CreateStream_record() {
            var record = new CreateStream("orders", 1024, RoutingAlgorithm.XxHash3V1);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_RegisterGroup_record_with_several_members() {
            var record = new RegisterGroup("group-1", [
                new ShardGroupMember("node-1", new Uri("https://node-1:5001")),
                new ShardGroupMember("node-2", new Uri("https://node-2:5001")),
                new ShardGroupMember("node-3", new Uri("https://node-3:5001")),
            ]);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().BeOfType<RegisterGroup>().Which.Members.Should().BeEquivalentTo(record.Members);
            ((RegisterGroup)decoded).GroupId.Should().Be("group-1");
        }

        [Fact]
        public void Round_trips_a_RegisterGroup_record_with_no_members() {
            var record = new RegisterGroup("group-1", []);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().BeOfType<RegisterGroup>().Which.Members.Should().BeEmpty();
            ((RegisterGroup)decoded).GroupId.Should().Be("group-1");
        }

        [Fact]
        public void Round_trips_a_MigrationStarted_record() {
            var record = new MigrationStarted(new MigrationId(Guid.NewGuid()), "orders", 5, "group-1", "group-2");

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_MigrationAborted_record() {
            var record = new MigrationAborted(new MigrationId(Guid.NewGuid()), "orders", 5);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_Cutover_record() {
            var record = new Cutover(new MigrationId(Guid.NewGuid()), "orders", 5, "group-2");

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_MigrationCompleted_record() {
            var record = new MigrationCompleted(new MigrationId(Guid.NewGuid()), "orders", 5);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_SetConsumerGroupAckMode_record() {
            var record = new SetConsumerGroupAckMode("orders", "billing", AckMode.Checkpoint);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_SetConsumerGroupMandatory_record() {
            var record = new SetConsumerGroupMandatory("orders", "billing", false);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_SetStreamRetentionPolicy_record_with_a_size_ceiling() {
            var record = new SetStreamRetentionPolicy("orders", TimeSpan.FromDays(3), TimeSpan.FromDays(14), MaxSizeBytesPerVirtualShard: 1_000_000_000);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_SetStreamRetentionPolicy_record_with_no_size_ceiling() {
            var record = new SetStreamRetentionPolicy("orders", TimeSpan.FromDays(3), TimeSpan.FromDays(14), MaxSizeBytesPerVirtualShard: null);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_SetStreamCheckpointInterval_record_with_both_triggers_set() {
            var record = new SetStreamCheckpointInterval("orders", TimeSpan.FromSeconds(30), 500);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Round_trips_a_SetStreamCheckpointInterval_record_with_both_triggers_null() {
            var record = new SetStreamCheckpointInterval("orders", null, null);

            var decoded = MetaLogRecord.Decode(MetaLogRecord.Encode(record));

            decoded.Should().Be(record);
        }

        [Fact]
        public void Throws_for_an_unrecognized_tag_byte() {
            var act = () => MetaLogRecord.Decode([255]);

            act.Should().Throw<ArgumentOutOfRangeException>();
        }
    }
}
