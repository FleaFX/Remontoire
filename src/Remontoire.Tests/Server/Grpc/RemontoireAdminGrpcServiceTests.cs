using System.Text;
using FluentAssertions;
using Grpc.Core;
using Remontoire.Admin.V1;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Server.Grpc;

// The RPC methods themselves need a real ServerCallContext (no fake precedent in this codebase,
// see RemontoireClientGrpcServiceTests's own remarks) — so only the extracted, pure logic is
// covered here. End-to-end coverage of CreateStream's own bootstrap sequence lives in a later,
// real-network harness once enough of the admin surface exists to justify one.
public class RemontoireAdminGrpcServiceTests {
    public class DeriveBootstrapMigrationId {
        [Fact]
        public void Is_deterministic_for_the_same_stream_and_virtual_shard() {
            var first = RemontoireAdminGrpcService.DeriveBootstrapMigrationId("orders", 3);
            var second = RemontoireAdminGrpcService.DeriveBootstrapMigrationId("orders", 3);

            second.Should().Be(first);
        }

        [Fact]
        public void Differs_for_a_different_virtual_shard_index() {
            var first = RemontoireAdminGrpcService.DeriveBootstrapMigrationId("orders", 3);
            var second = RemontoireAdminGrpcService.DeriveBootstrapMigrationId("orders", 4);

            second.Should().NotBe(first);
        }

        [Fact]
        public void Differs_for_a_different_stream_name() {
            var first = RemontoireAdminGrpcService.DeriveBootstrapMigrationId("orders", 3);
            var second = RemontoireAdminGrpcService.DeriveBootstrapMigrationId("billing", 3);

            second.Should().NotBe(first);
        }
    }

    public class MapRoutingAlgorithm {
        [Fact]
        public void Maps_XxHash3V1_to_its_C_sharp_counterpart() {
            RemontoireAdminGrpcService.MapRoutingAlgorithm(RoutingAlgorithmProto.XxHash3V1).Should().Be(RoutingAlgorithm.XxHash3V1);
        }

        [Fact]
        public void Rejects_unspecified_as_an_invalid_argument_rather_than_defaulting_silently() {
            var act = () => RemontoireAdminGrpcService.MapRoutingAlgorithm(RoutingAlgorithmProto.RoutingAlgorithmUnspecified);

            act.Should().Throw<RpcException>().Which.Status.StatusCode.Should().Be(StatusCode.InvalidArgument);
        }
    }

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

    public class CoversEveryIndexExactlyOnce {
        [Fact]
        public void Returns_true_when_every_index_is_present_exactly_once() {
            RemontoireAdminGrpcService.CoversEveryIndexExactlyOnce([0, 1, 2], virtualShardCount: 3).Should().BeTrue();
        }

        [Fact]
        public void Returns_false_when_an_index_is_missing() {
            RemontoireAdminGrpcService.CoversEveryIndexExactlyOnce([0, 1], virtualShardCount: 3).Should().BeFalse();
        }

        [Fact]
        public void Returns_false_when_an_index_is_duplicated_masking_a_missing_one() {
            RemontoireAdminGrpcService.CoversEveryIndexExactlyOnce([0, 1, 1], virtualShardCount: 3).Should().BeFalse();
        }

        [Fact]
        public void Returns_false_when_an_index_is_out_of_range() {
            RemontoireAdminGrpcService.CoversEveryIndexExactlyOnce([0, 1, 5], virtualShardCount: 3).Should().BeFalse();
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
        public void Describes_a_SetProduceAcl_record() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new SetProduceAcl("client-1", "orders", true));

            recordType.Should().Be("SetProduceAcl");
            fields.Should().Equal(new Dictionary<string, string> {
                ["Subject"] = "client-1", ["StreamName"] = "orders", ["Allowed"] = "True",
            });
        }

        [Fact]
        public void Describes_a_SetConsumeAcl_record() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new SetConsumeAcl("client-1", "orders", "billing", false));

            recordType.Should().Be("SetConsumeAcl");
            fields.Should().Equal(new Dictionary<string, string> {
                ["Subject"] = "client-1", ["StreamName"] = "orders", ["ConsumerGroup"] = "billing", ["Allowed"] = "False",
            });
        }

        [Fact]
        public void Describes_a_SetStreamSubjectClaimType_record_with_a_claim_type() {
            var (recordType, fields) = RemontoireAdminGrpcService.Describe(new SetStreamSubjectClaimType("orders", "client_id"));

            recordType.Should().Be("SetStreamSubjectClaimType");
            fields.Should().Equal(new Dictionary<string, string> { ["StreamName"] = "orders", ["ClaimType"] = "client_id" });
        }

        [Fact]
        public void Describes_a_SetStreamSubjectClaimType_record_with_no_claim_type() {
            var (_, fields) = RemontoireAdminGrpcService.Describe(new SetStreamSubjectClaimType("orders", null));

            fields["ClaimType"].Should().BeEmpty();
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
                new Header(Encoding.UTF8.GetBytes(RemontoireAdminGrpcService.ProposedByHeaderKey), Encoding.UTF8.GetBytes("client-1")),
            };

            RemontoireAdminGrpcService.FindProposedBy(headers).Should().Be("client-1");
        }

        [Fact]
        public void Ignores_headers_with_a_different_key() {
            var headers = new[] { new Header(Encoding.UTF8.GetBytes("correlation-id"), Encoding.UTF8.GetBytes("abc")) };

            RemontoireAdminGrpcService.FindProposedBy(headers).Should().BeNull();
        }
    }
}
