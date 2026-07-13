using FluentAssertions;
using Remontoire.Raft;

namespace Remontoire.Raft.Grpc;

public class RaftReplicaRegistryTests {
    [Fact]
    public void All_is_empty_when_nothing_is_registered() =>
        new RaftReplicaRegistry().All.Should().BeEmpty();

    [Fact]
    public async Task All_reflects_every_currently_registered_replica() {
        var registry = new RaftReplicaRegistry();
        await using var one = await StartSingleNodeReplicaAsync("group-1", "node-1");
        await using var two = await StartSingleNodeReplicaAsync("group-2", "node-2");
        registry.Register(one);
        registry.Register(two);

        registry.All.Should().BeEquivalentTo([one, two]);
    }

    [Fact]
    public async Task Unregistering_removes_the_replica_from_All() {
        var registry = new RaftReplicaRegistry();
        await using var replica = await StartSingleNodeReplicaAsync("group-1", "node-1");
        registry.Register(replica);

        registry.Unregister(replica.GroupId);

        registry.All.Should().BeEmpty();
    }

    static async Task<RaftReplica> StartSingleNodeReplicaAsync(string groupId, string nodeId) {
        var config = new RaftReplicaConfig(
            GroupId: groupId, NodeId: nodeId, Peers: [],
            HeartbeatInterval: TimeSpan.FromMilliseconds(50), ElectionTimeoutMin: TimeSpan.FromMilliseconds(150), ElectionTimeoutMax: TimeSpan.FromMilliseconds(300));
        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync();
        return replica;
    }
}
