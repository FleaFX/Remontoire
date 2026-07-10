using FluentAssertions;
using Microsoft.Extensions.Options;
using Remontoire.Raft.Grpc;

namespace Remontoire.Server;

public class RaftReplicaHostedServiceTests {
    public class StartAsync {
        [Fact]
        public async Task Registers_the_data_group_but_not_a_meta_group_when_none_is_configured() {
            var dataDirectory = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var messagingRegistry = new MessagingGroupRegistry();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }),
                    registry, messagingRegistry, new LeaderAddressDirectory());

                await service.StartAsync(CancellationToken.None);
                try {
                    registry.TryGet("group-1", out _).Should().BeTrue();
                    messagingRegistry.TryGet("group-1", out _).Should().BeTrue();
                    registry.TryGet("__meta__", out _).Should().BeFalse();
                } finally {
                    await service.StopAsync(CancellationToken.None);
                }
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
            }
        }

        [Fact]
        public async Task Registers_both_the_data_group_and_the_meta_group_when_configured() {
            var dataDirectory = CreateTempDirectory();
            var metaDirectory = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var messagingRegistry = new MessagingGroupRegistry();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory,
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, messagingRegistry, new LeaderAddressDirectory());

                await service.StartAsync(CancellationToken.None);
                try {
                    registry.TryGet("group-1", out _).Should().BeTrue();
                    registry.TryGet("__meta__", out var metaReplica).Should().BeTrue();
                    metaReplica.GroupId.Should().Be("__meta__");

                    // The meta-group holds no virtual shards of its own — it never gets a ShardLog/AckIndex.
                    messagingRegistry.TryGet("__meta__", out _).Should().BeFalse();
                } finally {
                    await service.StopAsync(CancellationToken.None);
                }
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
                Directory.Delete(metaDirectory, recursive: true);
            }
        }

        [Fact]
        public async Task Tolerates_the_same_peer_node_id_appearing_in_both_the_data_group_and_the_meta_group() {
            var dataDirectory = CreateTempDirectory();
            var metaDirectory = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory,
                        Peers = [new RaftPeerOptions { NodeId = "node-2", Address = "http://localhost:61001" }],
                        MetaGroup = new MetaGroupOptions {
                            NodeId = "node-1", DataDirectory = metaDirectory,
                            Peers = [new RaftPeerOptions { NodeId = "node-2", Address = "http://localhost:61001" }],
                        },
                    }),
                    registry, new MessagingGroupRegistry(), new LeaderAddressDirectory());

                var act = async () => await service.StartAsync(CancellationToken.None);

                await act.Should().NotThrowAsync("the same peer, shared by both groups, must collapse onto a single transport channel");
                await service.StopAsync(CancellationToken.None);
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
                Directory.Delete(metaDirectory, recursive: true);
            }
        }
    }

    public class StopAsync {
        [Fact]
        public async Task Unregisters_the_meta_group_alongside_the_data_group() {
            var dataDirectory = CreateTempDirectory();
            var metaDirectory = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory,
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, new MessagingGroupRegistry(), new LeaderAddressDirectory());

                await service.StartAsync(CancellationToken.None);
                await service.StopAsync(CancellationToken.None);

                registry.TryGet("group-1", out _).Should().BeFalse();
                registry.TryGet("__meta__", out _).Should().BeFalse();
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
                Directory.Delete(metaDirectory, recursive: true);
            }
        }
    }

    static string CreateTempDirectory() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }
}
