using FluentAssertions;
using Grpc.Core;
using Remontoire.Admin.V1;
using Remontoire.Sharding;

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
}
