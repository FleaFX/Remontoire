using System.Text;
using FluentAssertions;
using Grpc.Core;
using Remontoire.Admin.V1;
using Remontoire.Engine;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server;

// The RPC methods themselves need a real ServerCallContext (no fake precedent in this codebase,
// see RemontoireClientGrpcServiceTests's own remarks) — so only the extracted, pure logic is
// covered here. End-to-end coverage of EnsureStream's own bootstrap sequence lives in
// RemontoireAdminGrpcServiceEndToEndTests.
public class RemontoireAdminGrpcServiceTests {
    public class MapAckMode {
        [Fact]
        public void Maps_Strict_to_its_C_sharp_counterpart() {
            RemontoireAdminGrpcService.MapAckMode(AckModeProto.Strict).Should().Be(AckMode.Strict);
        }

        [Fact]
        public void Maps_Checkpoint_to_its_C_sharp_counterpart() {
            RemontoireAdminGrpcService.MapAckMode(AckModeProto.Checkpoint).Should().Be(AckMode.Checkpoint);
        }

        [Fact]
        public void Rejects_unspecified_as_an_invalid_argument_rather_than_defaulting_silently() {
            var act = () => RemontoireAdminGrpcService.MapAckMode(AckModeProto.AckModeUnspecified);

            act.Should().Throw<RpcException>().Which.Status.StatusCode.Should().Be(StatusCode.InvalidArgument);
        }
    }

    public class ParseMigrationId {
        [Fact]
        public void Parses_a_well_formed_guid_string() {
            var guid = Guid.NewGuid();

            RemontoireAdminGrpcService.ParseMigrationId(guid.ToString()).Value.Should().Be(guid);
        }

        [Fact]
        public void Rejects_a_malformed_value_as_an_invalid_argument_rather_than_an_unhandled_FormatException() {
            var act = () => RemontoireAdminGrpcService.ParseMigrationId("not-a-guid");

            act.Should().Throw<RpcException>().Which.Status.StatusCode.Should().Be(StatusCode.InvalidArgument);
        }
    }

    public class Describe {
        [Fact]
        public void Describes_a_CreateStream_record() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new CreateStream("orders", 1024, RoutingAlgorithm.XxHash3V1));

            recordType.Should().Be("CreateStream");
            fields.Should().Equal(new Dictionary<string, string> {
                ["StreamName"] = "orders", ["VirtualShardCount"] = "1024", ["RoutingAlgorithm"] = "XxHash3V1",
            });
        }

        [Fact]
        public void Describes_a_RegisterGroup_record() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new RegisterGroup("group-1", [
                new ShardGroupMember("node-1", new Uri("https://node-1:5001")),
                new ShardGroupMember("node-2", new Uri("https://node-2:5001")),
            ]));

            recordType.Should().Be("RegisterGroup");
            fields.Should().Equal(new Dictionary<string, string> {
                ["GroupId"] = "group-1", ["Members"] = "node-1@https://node-1:5001/, node-2@https://node-2:5001/",
            });
        }

        [Fact]
        public void Describes_a_MigrationStarted_record() {
            var migrationId = new MigrationId(Guid.NewGuid());
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new MigrationStarted(migrationId, "orders", 5, "group-1", "group-2"));

            recordType.Should().Be("MigrationStarted");
            fields.Should().Equal(new Dictionary<string, string> {
                ["MigrationId"] = migrationId.Value.ToString(), ["StreamName"] = "orders", ["VirtualShardIndex"] = "5",
                ["FromGroupId"] = "group-1", ["ToGroupId"] = "group-2",
            });
        }

        [Fact]
        public void Describes_a_MigrationAborted_record() {
            var migrationId = new MigrationId(Guid.NewGuid());
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new MigrationAborted(migrationId, "orders", 5));

            recordType.Should().Be("MigrationAborted");
            fields.Should().Equal(new Dictionary<string, string> {
                ["MigrationId"] = migrationId.Value.ToString(), ["StreamName"] = "orders", ["VirtualShardIndex"] = "5",
            });
        }

        [Fact]
        public void Describes_a_Cutover_record() {
            var migrationId = new MigrationId(Guid.NewGuid());
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new Cutover(migrationId, "orders", 5, "group-2"));

            recordType.Should().Be("Cutover");
            fields.Should().Equal(new Dictionary<string, string> {
                ["MigrationId"] = migrationId.Value.ToString(), ["StreamName"] = "orders", ["VirtualShardIndex"] = "5", ["ToGroupId"] = "group-2",
            });
        }

        [Fact]
        public void Describes_a_MigrationCompleted_record() {
            var migrationId = new MigrationId(Guid.NewGuid());
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new MigrationCompleted(migrationId, "orders", 5));

            recordType.Should().Be("MigrationCompleted");
            fields.Should().Equal(new Dictionary<string, string> {
                ["MigrationId"] = migrationId.Value.ToString(), ["StreamName"] = "orders", ["VirtualShardIndex"] = "5",
            });
        }

        [Fact]
        public void Describes_a_SetConsumerGroupAckMode_record() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new SetConsumerGroupAckMode("orders", "billing", AckMode.Checkpoint));

            recordType.Should().Be("SetConsumerGroupAckMode");
            fields.Should().Equal(new Dictionary<string, string> {
                ["StreamName"] = "orders", ["ConsumerGroup"] = "billing", ["Mode"] = "Checkpoint",
            });
        }

        [Fact]
        public void Describes_a_SetConsumerGroupMandatory_record() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new SetConsumerGroupMandatory("orders", "billing", false));

            recordType.Should().Be("SetConsumerGroupMandatory");
            fields.Should().Equal(new Dictionary<string, string> {
                ["StreamName"] = "orders", ["ConsumerGroup"] = "billing", ["Mandatory"] = "False",
            });
        }

        [Fact]
        public void Describes_a_SetStreamRetentionPolicy_record_with_a_size_ceiling() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(
                new SetStreamRetentionPolicy("orders", TimeSpan.FromDays(3), TimeSpan.FromDays(14), MaxSizeBytesPerVirtualShard: 1_000_000_000));

            recordType.Should().Be("SetStreamRetentionPolicy");
            fields.Should().Equal(new Dictionary<string, string> {
                ["StreamName"] = "orders", ["AuditRetention"] = TimeSpan.FromDays(3).ToString(), ["MaxRetention"] = TimeSpan.FromDays(14).ToString(),
                ["MaxSizeBytesPerVirtualShard"] = "1000000000",
            });
        }

        [Fact]
        public void Describes_a_SetStreamRetentionPolicy_record_with_no_size_ceiling() {
            var (_, fields) = RemontoireAdminGrpcService.Describe(
                new SetStreamRetentionPolicy("orders", TimeSpan.FromDays(3), TimeSpan.FromDays(14), MaxSizeBytesPerVirtualShard: null));

            fields["MaxSizeBytesPerVirtualShard"].Should().BeEmpty();
        }

        [Fact]
        public void Describes_a_SetStreamCheckpointInterval_record_with_both_triggers_set() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new SetStreamCheckpointInterval("orders", TimeSpan.FromSeconds(30), 500));

            recordType.Should().Be("SetStreamCheckpointInterval");
            fields.Should().Equal(new Dictionary<string, string> {
                ["StreamName"] = "orders", ["Interval"] = TimeSpan.FromSeconds(30).ToString(), ["OffsetCount"] = "500",
            });
        }

        [Fact]
        public void Describes_a_SetStreamCheckpointInterval_record_with_both_triggers_null() {
            var (_, fields) = RemontoireAdminGrpcService.Describe(new SetStreamCheckpointInterval("orders", null, null));

            fields["Interval"].Should().BeEmpty();
            fields["OffsetCount"].Should().BeEmpty();
        }

        [Fact]
        public void Describes_a_DeclareConsumerGroup_record() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new DeclareConsumerGroup("orders", "billing", AckMode.Checkpoint, Mandatory: false));

            recordType.Should().Be("DeclareConsumerGroup");
            fields.Should().Equal(new Dictionary<string, string> {
                ["StreamName"] = "orders", ["ConsumerGroup"] = "billing", ["Mode"] = AckMode.Checkpoint.ToString(), ["Mandatory"] = false.ToString(),
            });
        }
    }

    public class FindProposedBy {
        [Fact]
        public void Returns_null_when_no_header_carries_the_proposed_by_key() {
            RemontoireAdminGrpcService.FindProposedBy([]).Should().BeNull();
        }

        [Fact]
        public void Returns_the_value_of_the_matching_header() {
            var headers = new[] {
                new Header(Encoding.UTF8.GetBytes(RemontoireNode.ProposedByHeaderKey), Encoding.UTF8.GetBytes("client-1")),
            };

            RemontoireAdminGrpcService.FindProposedBy(headers).Should().Be("client-1");
        }

        [Fact]
        public void Ignores_headers_with_a_different_key() {
            var headers = new[] { new Header(Encoding.UTF8.GetBytes("correlation-id"), Encoding.UTF8.GetBytes("abc")) };

            RemontoireAdminGrpcService.FindProposedBy(headers).Should().BeNull();
        }
    }

    public class ToStatus {
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void Maps_each_exception_to_its_status_code(int caseIndex) {
            Exception exception = caseIndex switch {
                0 => new ArgumentException("invalid"),
                1 => new RemontoireConflictException("conflict"),
                2 => new RemontoireUnavailableException("group-1", 1),
                3 => new TimeoutException(),
                _ => new InvalidOperationException("no replica"),
            };
            var expected = caseIndex switch {
                0 => StatusCode.InvalidArgument,
                1 => StatusCode.AlreadyExists,
                2 => StatusCode.Unavailable,
                3 => StatusCode.Unavailable,
                _ => StatusCode.FailedPrecondition,
            };

            RemontoireAdminGrpcService.ToStatus(exception).Should().Be(new Status(expected, exception.Message));
        }
    }
}
