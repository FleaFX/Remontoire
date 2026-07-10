using FluentAssertions;
using Microsoft.Extensions.Options;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

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
                    Options.Create(new RaftServerOptions { Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }] }),
                    registry, messagingRegistry, new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal());

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
        public async Task Registers_every_configured_data_group() {
            var directoryA = CreateTempDirectory();
            var directoryB = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var messagingRegistry = new MessagingGroupRegistry();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        Groups = [
                            new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = directoryA },
                            new RaftGroupOptions { GroupId = "group-2", NodeId = "node-1", DataDirectory = directoryB },
                        ],
                    }),
                    registry, messagingRegistry, new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal());

                await service.StartAsync(CancellationToken.None);
                try {
                    registry.TryGet("group-1", out _).Should().BeTrue();
                    registry.TryGet("group-2", out _).Should().BeTrue();
                    messagingRegistry.TryGet("group-1", out _).Should().BeTrue();
                    messagingRegistry.TryGet("group-2", out _).Should().BeTrue();
                } finally {
                    await service.StopAsync(CancellationToken.None);
                }
            } finally {
                Directory.Delete(directoryA, recursive: true);
                Directory.Delete(directoryB, recursive: true);
            }
        }

        [Fact]
        public async Task Registers_both_a_data_group_and_the_meta_group_when_configured() {
            var dataDirectory = CreateTempDirectory();
            var metaDirectory = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var messagingRegistry = new MessagingGroupRegistry();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, messagingRegistry, new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal());

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
        public async Task Feeds_the_shared_assignment_table_when_this_node_hosts_the_meta_group() {
            var dataDirectory = CreateTempDirectory();
            var metaDirectory = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var table = new ShardAssignmentTable();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, new MessagingGroupRegistry(), new LeaderAddressDirectory(), table, new MetaLogJournal());

                await service.StartAsync(CancellationToken.None);
                try {
                    registry.TryGet("__meta__", out var metaReplica);
                    metaReplica.TryPost(new ElectionTimeoutElapsed(metaReplica.ElectionTimerGeneration)); // single-node -> ready leader
                    await metaReplica.DrainAsync();

                    await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream("orders", 1024, RoutingAlgorithm.XxHash3V1))));

                    (await WaitUntilAsync(() => table.TryGetStreamConfig("orders", out _))).Should().BeTrue();
                } finally {
                    await service.StopAsync(CancellationToken.None);
                }
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
                Directory.Delete(metaDirectory, recursive: true);
            }
        }

        [Fact]
        public async Task Tolerates_the_same_peer_node_id_appearing_in_both_a_data_group_and_the_meta_group() {
            var dataDirectory = CreateTempDirectory();
            var metaDirectory = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        Groups = [
                            new RaftGroupOptions {
                                GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory,
                                Peers = [new RaftPeerOptions { NodeId = "node-2", Address = "http://localhost:61001" }],
                            },
                        ],
                        MetaGroup = new MetaGroupOptions {
                            NodeId = "node-1", DataDirectory = metaDirectory,
                            Peers = [new RaftPeerOptions { NodeId = "node-2", Address = "http://localhost:61001" }],
                        },
                    }),
                    registry, new MessagingGroupRegistry(), new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal());

                var act = async () => await service.StartAsync(CancellationToken.None);

                await act.Should().NotThrowAsync("the same peer, shared by both groups, must collapse onto a single transport channel");
                await service.StopAsync(CancellationToken.None);
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
                Directory.Delete(metaDirectory, recursive: true);
            }
        }

        [Fact]
        public async Task Does_not_throw_when_bootstrapping_a_watcher_from_meta_group_seed_addresses() {
            var dataDirectory = CreateTempDirectory();
            try {
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroupSeedAddresses = ["http://localhost:61002"],
                    }),
                    new RaftReplicaRegistry(), new MessagingGroupRegistry(), new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal());

                var act = async () => await service.StartAsync(CancellationToken.None);

                await act.Should().NotThrowAsync("channel construction is lazy — no connection is attempted yet");
                await service.StopAsync(CancellationToken.None);
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
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
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, new MessagingGroupRegistry(), new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal());

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

    static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null) {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(2));
        while (DateTime.UtcNow < deadline) {
            if (condition())
                return true;

            await Task.Delay(5);
        }

        return condition();
    }
}
