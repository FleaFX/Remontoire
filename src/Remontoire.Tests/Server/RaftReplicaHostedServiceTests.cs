using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Security;
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
                    Options.Create(new RaftServerOptions {
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true }, Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }] }),
                    registry, messagingRegistry, new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

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
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true },
                        Groups = [
                            new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = directoryA },
                            new RaftGroupOptions { GroupId = "group-2", NodeId = "node-1", DataDirectory = directoryB },
                        ],
                    }),
                    registry, messagingRegistry, new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

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
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true },
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, messagingRegistry, new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

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
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true },
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, new MessagingGroupRegistry(), new LeaderAddressDirectory(), table, new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

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
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true },
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
                    registry, new MessagingGroupRegistry(), new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

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
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true },
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroupSeedAddresses = ["http://localhost:61002"],
                    }),
                    new RaftReplicaRegistry(), new MessagingGroupRegistry(), new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

                var act = async () => await service.StartAsync(CancellationToken.None);

                await act.Should().NotThrowAsync("channel construction is lazy — no connection is attempted yet");
                await service.StopAsync(CancellationToken.None);
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
            }
        }
    }

    // AckCheckpointer/RetentionEvaluator both depend on ResolveStreamNameForGroup,
    // which reverse-scans the shared ShardAssignmentTable — empty at the moment StartAsync
    // constructs these components, since the meta-group/watcher section runs after the data-group
    // loop, not before. AckCheckpointer's own 1-second tick makes an end-to-end wait practical;
    // RetentionEvaluator/SizePruneWorker's own multi-minute ticks don't, but they resolve the
    // stream name through the exact same ResolveStreamNameForGroup mechanism this test proves works.
    public class AckRetentionWiring {
        [Fact]
        public async Task Starts_and_stops_cleanly_with_every_fase_6_component_wired_in() {
            var dataDirectory = CreateTempDirectory();
            var metaDirectory = CreateTempDirectory();
            try {
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true },
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    new RaftReplicaRegistry(), new MessagingGroupRegistry(), new LeaderAddressDirectory(), new ShardAssignmentTable(),
                    new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

                await service.StartAsync(CancellationToken.None);
                await service.StopAsync(CancellationToken.None);
                // No hang, no exception — AckCheckpointer/RetentionEvaluator disposed before the
                // ShardLog/RaftReplica they depend on, same ordering AckIndexApplier already follows.
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
                Directory.Delete(metaDirectory, recursive: true);
            }
        }

        [Fact]
        public async Task AckCheckpointer_self_heals_once_the_assignment_table_resolves_this_groups_stream_name() {
            var dataDirectory = CreateTempDirectory();
            var metaDirectory = CreateTempDirectory();
            try {
                var registry = new RaftReplicaRegistry();
                var messagingRegistry = new MessagingGroupRegistry();
                var table = new ShardAssignmentTable();
                var service = new RaftReplicaHostedService(
                    Options.Create(new RaftServerOptions {
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true },
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, messagingRegistry, new LeaderAddressDirectory(), table, new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

                await service.StartAsync(CancellationToken.None);
                try {
                    // At this point ResolveStreamNameForGroup("group-1") has nothing to find yet —
                    // the meta-group replica above only just started, empty.
                    registry.TryGet("__meta__", out var metaReplica);
                    metaReplica.TryPost(new ElectionTimeoutElapsed(metaReplica.ElectionTimerGeneration));
                    await metaReplica.DrainAsync();

                    var migrationId = new MigrationId(Guid.NewGuid());
                    await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new CreateStream("orders", 1, RoutingAlgorithm.XxHash3V1))));
                    await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new RegisterGroup("group-1", []))));
                    await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new MigrationStarted(migrationId, "orders", 0, "group-1", "group-1"))));
                    await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new Cutover(migrationId, "orders", 0, "group-1"))));
                    await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new SetConsumerGroupAckMode("orders", "checkpoint-group", AckMode.Checkpoint))));
                    await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(new SetStreamCheckpointInterval("orders", Interval: null, OffsetCount: 1))));

                    (await WaitUntilAsync(() => table.TryGetAssignment("orders", 0, out var assignment) && assignment.GroupId == "group-1"))
                        .Should().BeTrue("the assignment must resolve before ResolveStreamNameForGroup can ever find it");

                    messagingRegistry.TryGet("group-1", out var messaging).Should().BeTrue();
                    await messaging.AckIndex.ApplyLocalAsync("checkpoint-group", [0]);

                    // Proves the whole chain: ResolveStreamNameForGroup found "orders" for "group-1",
                    // isCheckpointMode saw AckMode.Checkpoint for "checkpoint-group", and
                    // getCheckpointThresholds saw the OffsetCount:1 trigger — AckCheckpointer's own
                    // 1-second tick proposes a real AckCheckpoint, which AckIndexApplier replays
                    // back into the very same AckIndex, advancing CommittedWatermark.
                    (await WaitUntilAsync(() => messaging.AckIndex.GetOrCreate("checkpoint-group").CommittedWatermark == 1, TimeSpan.FromSeconds(10)))
                        .Should().BeTrue();
                } finally {
                    await service.StopAsync(CancellationToken.None);
                }
            } finally {
                Directory.Delete(dataDirectory, recursive: true);
                Directory.Delete(metaDirectory, recursive: true);
            }
        }
    }

    // Regression coverage for a confirmed review bug: once a dead-letter stream is provisioned
    // onto the same physical group as its own source stream (§4.4's own v1 convention),
    // ResolveStreamNameForGroup's reverse-scan finds two candidates for that group. A plain
    // FirstOrDefault over ConcurrentDictionary's unordered enumeration could resolve either one —
    // tested directly against the extracted tie-break rule, independent of the table's actual
    // (non-deterministic) iteration order.
    public class PickPrimaryStreamName {
        [Fact]
        public void Prefers_the_real_stream_when_the_dead_letter_shadow_is_enumerated_first() {
            var candidates = new[] {
                new VirtualShardAssignment("orders.__deadletter__", 0, "group-1"),
                new VirtualShardAssignment("orders", 0, "group-1"),
            };

            RaftReplicaHostedService.PickPrimaryStreamName(candidates).Should().Be("orders");
        }

        [Fact]
        public void Prefers_the_real_stream_when_the_dead_letter_shadow_is_enumerated_last() {
            var candidates = new[] {
                new VirtualShardAssignment("orders", 0, "group-1"),
                new VirtualShardAssignment("orders.__deadletter__", 0, "group-1"),
            };

            RaftReplicaHostedService.PickPrimaryStreamName(candidates).Should().Be("orders");
        }

        [Fact]
        public void Falls_back_to_the_dead_letter_stream_when_it_is_the_only_candidate() {
            var candidates = new[] { new VirtualShardAssignment("orders.__deadletter__", 0, "group-1") };

            RaftReplicaHostedService.PickPrimaryStreamName(candidates).Should().Be("orders.__deadletter__");
        }

        [Fact]
        public void Returns_null_for_no_candidates() {
            RaftReplicaHostedService.PickPrimaryStreamName([]).Should().BeNull();
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
                        Mtls = new ClusterMtlsOptions { AllowInsecureTransport = true },
                        Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = "node-1", DataDirectory = dataDirectory }],
                        MetaGroup = new MetaGroupOptions { NodeId = "node-1", DataDirectory = metaDirectory },
                    }),
                    registry, new MessagingGroupRegistry(), new LeaderAddressDirectory(), new ShardAssignmentTable(), new MetaLogJournal(), new MigrationAdmissionGate(), NullLoggerFactory.Instance);

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
