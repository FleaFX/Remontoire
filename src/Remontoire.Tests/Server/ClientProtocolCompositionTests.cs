using FluentAssertions;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Storage;

namespace Remontoire.Server;

// Layer 2/3-equivalent for the client protocol: composes a real RaftReplica + ShardLog + AckIndex
// the same way RaftReplicaHostedService does, then calls ProposeAsync/TryGet/AckIndex directly as
// plain C# method calls — no gRPC layer, no RemontoireClientGrpcService. Proves the composition
// itself is wired correctly before a real network sits on top of it.
public class ClientProtocolCompositionTests {
    [Fact]
    public async Task A_published_message_becomes_visible_through_the_composed_ShardLog() {
        var directory = CreateTempDirectory();
        try {
            var (replica, shardLog, _, applier, _) = await ComposeAsync(directory);
            try {
                var result = await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));

                (await WaitUntilAsync(() => shardLog.TryGet(result.LogicalOffset, out _))).Should().BeTrue();
            } finally {
                await applier.DisposeAsync();
                await shardLog.DisposeAsync();
            }
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task An_acked_offset_is_reflected_in_the_composed_AckIndex() {
        var directory = CreateTempDirectory();
        try {
            var (replica, shardLog, ackIndex, applier, _) = await ComposeAsync(directory);
            try {
                var published = await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));
                await replica.ProposeAsync(new AckRequest("group-1", [published.LogicalOffset]));

                (await WaitUntilAsync(() => ackIndex.GetOrCreate("group-1").IsAcked(published.LogicalOffset))).Should().BeTrue();
            } finally {
                await applier.DisposeAsync();
                await shardLog.DisposeAsync();
            }
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task An_Ack_proposal_never_shifts_a_subsequent_Append_LogicalOffset() {
        var directory = CreateTempDirectory();
        try {
            var (replica, shardLog, _, applier, _) = await ComposeAsync(directory);
            try {
                await replica.ProposeAsync(new AckRequest("group-1", [0]));
                var appendResult = await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));

                appendResult.LogicalOffset.Should().Be(0);
            } finally {
                await applier.DisposeAsync();
                await shardLog.DisposeAsync();
            }
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Ack_driven_pruning_reads_the_watermark_through_the_same_CompactionPolicy_delegate_RaftReplicaHostedService_wires() {
        var directory = CreateTempDirectory();
        try {
            var (replica, shardLog, ackIndex, applier, compactionPolicy) = await ComposeAsync(directory);
            try {
                var result = await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));
                await replica.ProposeAsync(new AckRequest("group-1", [result.LogicalOffset]));
                (await WaitUntilAsync(() => ackIndex.AllGroupsLowWatermark() > result.LogicalOffset)).Should().BeTrue();

                var watermarkThroughPolicy = await compactionPolicy.GetAckedLowWatermarkAsync!(CancellationToken.None);

                watermarkThroughPolicy.Should().Be(result.LogicalOffset + 1, "exclusive — the single published offset is now fully acked");
            } finally {
                await applier.DisposeAsync();
                await shardLog.DisposeAsync();
            }
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    static async Task<(RaftReplica Replica, ShardLog ShardLog, AckIndex AckIndex, AckIndexApplier Applier, CompactionPolicy CompactionPolicy)> ComposeAsync(string directory) {
        var config = new RaftReplicaConfig(
            GroupId: "group-1", NodeId: "node-1", Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync();
        replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node group -> ready leader
        await replica.DrainAsync();

        var ackIndex = new AckIndex();
        var compactionPolicy = new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null,
            GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark()));
        var shardLog = await ShardLog.OpenAsync(directory, replica.ReadCommittedAsync, compactionPolicy: compactionPolicy);
        var applier = new AckIndexApplier(shardLog, ackIndex);

        return (replica, shardLog, ackIndex, applier, compactionPolicy);
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
