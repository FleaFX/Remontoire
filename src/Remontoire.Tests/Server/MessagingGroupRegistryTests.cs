using System.Runtime.CompilerServices;
using FluentAssertions;
using Remontoire.Messaging;
using Remontoire.Storage;

namespace Remontoire.Server;

public class MessagingGroupRegistryTests {
    [Fact]
    public void All_is_empty_when_nothing_is_registered() =>
        new MessagingGroupRegistry().All.Should().BeEmpty();

    [Fact]
    public async Task All_reflects_every_currently_registered_group() {
        var registry = new MessagingGroupRegistry();
        await using var one = await ComposeAsync();
        await using var two = await ComposeAsync();
        registry.Register("group-1", one.ShardLog, one.AckIndex, one.RetentionEvaluator);
        registry.Register("group-2", two.ShardLog, two.AckIndex, two.RetentionEvaluator);

        registry.All.Should().BeEquivalentTo([
            ("group-1", one.ShardLog, one.AckIndex, one.RetentionEvaluator),
            ("group-2", two.ShardLog, two.AckIndex, two.RetentionEvaluator),
        ]);
    }

    [Fact]
    public async Task Unregistering_removes_the_group_from_All() {
        var registry = new MessagingGroupRegistry();
        await using var group = await ComposeAsync();
        registry.Register("group-1", group.ShardLog, group.AckIndex, group.RetentionEvaluator);

        registry.Unregister("group-1");

        registry.All.Should().BeEmpty();
    }

    static async Task<Composed> ComposeAsync() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        var ackIndex = new AckIndex();
        var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSourceAsync,
            compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())));
        var retentionEvaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
            ShardLog: shardLog, AckIndex: ackIndex, IsMandatory: _ => true, GetMaxRetention: () => TimeSpan.MaxValue,
            ForwardToDeadLetterAsync: (_, _) => Task.FromResult(false), IsAdmissionPaused: () => false, IsLeader: () => true));
        return new Composed(directory, shardLog, ackIndex, retentionEvaluator);
    }

    static async IAsyncEnumerable<WalRecord> EmptyCommittedSourceAsync([EnumeratorCancellation] CancellationToken cancellationToken) {
        await Task.CompletedTask;
        yield break;
    }

    sealed record Composed(string DirectoryPath, ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator) : IAsyncDisposable {
        public async ValueTask DisposeAsync() {
            await RetentionEvaluator.DisposeAsync();
            await ShardLog.DisposeAsync();
            Directory.Delete(DirectoryPath, recursive: true);
        }
    }
}
