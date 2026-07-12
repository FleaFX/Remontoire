using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Remontoire.Storage;

namespace Remontoire.Messaging;

// Layer 3-ish: a real ShardLog + AckIndex, a test-double forwardToDeadLetterAsync — no Raft, no gRPC.
public class RetentionEvaluatorTests {
    static readonly TimeSpan TickInterval = TimeSpan.FromMinutes(1);
    static readonly TimeSpan MaxRetention = TimeSpan.FromHours(1);

    [Fact]
    public async Task Forwards_a_message_past_max_retention_that_no_mandatory_group_has_acked() {
        var directory = CreateTempDirectory();
        try {
            var timeProvider = new FakeTimeProvider();
            var oldTimestamp = ToMicros(timeProvider.GetUtcNow() - MaxRetention - TimeSpan.FromHours(1));
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            var forwarded = new ConcurrentQueue<AppendRequest>();

            shardLog.TryPost(new WalRecordCommitted(AppendRecord(0, oldTimestamp, "hello world")));
            await WaitForVisibleAsync(shardLog, 0);

            await using var evaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex, IsMandatory: _ => true, GetMaxRetention: () => MaxRetention,
                ForwardToDeadLetterAsync: Forward(forwarded), IsAdmissionPaused: () => false, IsLeader: () => true, timeProvider));

            await AdvanceAndSettleAsync(timeProvider);

            forwarded.Should().ContainSingle();
            Encoding.UTF8.GetString(forwarded.Single().Payload.Span).Should().Be("hello world");
            evaluator.DeadLetterMessagesTotal.Should().Be(1);
            evaluator.SafeToPruneWatermark.Should().Be(1);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Never_forwards_a_message_a_mandatory_group_already_acked() {
        var directory = CreateTempDirectory();
        try {
            var timeProvider = new FakeTimeProvider();
            var oldTimestamp = ToMicros(timeProvider.GetUtcNow() - MaxRetention - TimeSpan.FromHours(1));
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            ackIndex.Apply(AckRecord("mandatory", 0)); // a genuinely committed ack — ApplyLocal alone would never advance CommittedWatermark
            var forwarded = new ConcurrentQueue<AppendRequest>();

            shardLog.TryPost(new WalRecordCommitted(AppendRecord(0, oldTimestamp, "hello world")));
            await WaitForVisibleAsync(shardLog, 0);

            await using var evaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex, IsMandatory: consumerGroup => consumerGroup == "mandatory", GetMaxRetention: () => MaxRetention,
                ForwardToDeadLetterAsync: Forward(forwarded), IsAdmissionPaused: () => false, IsLeader: () => true, timeProvider));

            await AdvanceAndSettleAsync(timeProvider);

            forwarded.Should().BeEmpty("the mandatory group already acked this offset — nothing to force");
            evaluator.DeadLetterMessagesTotal.Should().Be(0);
            evaluator.SafeToPruneWatermark.Should().Be(1, "already covered by the mandatory watermark, still safe to prune");
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Does_not_forward_or_advance_the_watermark_within_the_retention_window() {
        var directory = CreateTempDirectory();
        try {
            var timeProvider = new FakeTimeProvider();
            var recentTimestamp = ToMicros(timeProvider.GetUtcNow()); // well within MaxRetention
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            var forwarded = new ConcurrentQueue<AppendRequest>();

            shardLog.TryPost(new WalRecordCommitted(AppendRecord(0, recentTimestamp, "hello world")));
            await WaitForVisibleAsync(shardLog, 0);

            await using var evaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex, IsMandatory: _ => true, GetMaxRetention: () => MaxRetention,
                ForwardToDeadLetterAsync: Forward(forwarded), IsAdmissionPaused: () => false, IsLeader: () => true, timeProvider));

            await AdvanceAndSettleAsync(timeProvider);

            forwarded.Should().BeEmpty();
            evaluator.SafeToPruneWatermark.Should().Be(0, "this message is still within its audit window — nothing may prune past it yet");
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Ignores_best_effort_groups_when_deciding_a_message_is_already_safe() {
        var directory = CreateTempDirectory();
        try {
            var timeProvider = new FakeTimeProvider();
            var oldTimestamp = ToMicros(timeProvider.GetUtcNow() - MaxRetention - TimeSpan.FromHours(1));
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            ackIndex.Apply(AckRecord("mandatory", 0)); // a genuinely committed ack — best-effort group never acks anything
            var forwarded = new ConcurrentQueue<AppendRequest>();

            shardLog.TryPost(new WalRecordCommitted(AppendRecord(0, oldTimestamp, "hello world")));
            await WaitForVisibleAsync(shardLog, 0);

            await using var evaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex, IsMandatory: consumerGroup => consumerGroup == "mandatory", GetMaxRetention: () => MaxRetention,
                ForwardToDeadLetterAsync: Forward(forwarded), IsAdmissionPaused: () => false, IsLeader: () => true, timeProvider));

            await AdvanceAndSettleAsync(timeProvider);

            forwarded.Should().BeEmpty("a stuck best-effort group must never force a dead-letter forward");
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Still_advances_the_watermark_for_already_acked_messages_when_max_retention_is_unbounded() {
        var directory = CreateTempDirectory();
        try {
            var timeProvider = new FakeTimeProvider();
            var oldTimestamp = ToMicros(timeProvider.GetUtcNow() - TimeSpan.FromDays(365));
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            ackIndex.Apply(AckRecord("mandatory", 0)); // a genuinely committed ack — already safe to prune regardless of retention
            var forwarded = new ConcurrentQueue<AppendRequest>();

            shardLog.TryPost(new WalRecordCommitted(AppendRecord(0, oldTimestamp, "hello world")));
            await WaitForVisibleAsync(shardLog, 0);

            // TimeSpan.MaxValue is both RaftReplicaHostedService's own fallback for an unresolved
            // stream name and a natural operator choice for "never time-expire, only by ack/size" —
            // computing utcNow - MaxValue must not throw and silently kill every future tick.
            await using var evaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex, IsMandatory: consumerGroup => consumerGroup == "mandatory", GetMaxRetention: () => TimeSpan.MaxValue,
                ForwardToDeadLetterAsync: Forward(forwarded), IsAdmissionPaused: () => false, IsLeader: () => true, timeProvider));

            await AdvanceAndSettleAsync(timeProvider);
            await AdvanceAndSettleAsync(timeProvider);

            forwarded.Should().BeEmpty("unbounded retention must never force a dead-letter forward");
            evaluator.SafeToPruneWatermark.Should().Be(1, "already covered by the mandatory watermark — an unbounded MaxRetention must not stall this");
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Never_forwards_on_a_follower_even_past_max_retention() {
        // Only the leader may propose the dead-letter forward. Without this gate, a follower
        // retries a doomed propose every tick forever (swallowed by the tick's own catch-all),
        // permanently stuck at the first such offset instead of just waiting for the leader's
        // own forward to eventually replicate through.
        var directory = CreateTempDirectory();
        try {
            var timeProvider = new FakeTimeProvider();
            var oldTimestamp = ToMicros(timeProvider.GetUtcNow() - MaxRetention - TimeSpan.FromHours(1));
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            var forwarded = new ConcurrentQueue<AppendRequest>();

            shardLog.TryPost(new WalRecordCommitted(AppendRecord(0, oldTimestamp, "hello world")));
            await WaitForVisibleAsync(shardLog, 0);

            await using var evaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex, IsMandatory: _ => true, GetMaxRetention: () => MaxRetention,
                ForwardToDeadLetterAsync: Forward(forwarded), IsAdmissionPaused: () => false, IsLeader: () => false, timeProvider));

            await AdvanceAndSettleAsync(timeProvider);

            forwarded.Should().BeEmpty("only the leader may propose a dead-letter forward");
            evaluator.DeadLetterMessagesTotal.Should().Be(0);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Never_advances_the_watermark_when_the_forward_silently_did_nothing() {
        // A false return means the dead-letter stream wasn't provisioned, the stream name couldn't
        // be resolved, or the target group isn't hosted locally — all documented, silent v1 gaps.
        // The message must NOT be treated as safe to prune in that case: no copy exists anywhere.
        var directory = CreateTempDirectory();
        try {
            var timeProvider = new FakeTimeProvider();
            var oldTimestamp = ToMicros(timeProvider.GetUtcNow() - MaxRetention - TimeSpan.FromHours(1));
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();

            shardLog.TryPost(new WalRecordCommitted(AppendRecord(0, oldTimestamp, "hello world")));
            await WaitForVisibleAsync(shardLog, 0);

            await using var evaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex, IsMandatory: _ => true, GetMaxRetention: () => MaxRetention,
                ForwardToDeadLetterAsync: (_, _) => Task.FromResult(false), IsAdmissionPaused: () => false, IsLeader: () => true, timeProvider));

            await AdvanceAndSettleAsync(timeProvider);

            evaluator.DeadLetterMessagesTotal.Should().Be(0, "no copy was actually made — this must not be counted as a successful dead-letter");
            evaluator.SafeToPruneWatermark.Should().Be(0, "the message was never actually preserved anywhere — it is not safe to prune");
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Never_ticks_while_admission_is_paused() {
        var directory = CreateTempDirectory();
        try {
            var timeProvider = new FakeTimeProvider();
            var oldTimestamp = ToMicros(timeProvider.GetUtcNow() - MaxRetention - TimeSpan.FromHours(1));
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            var forwarded = new ConcurrentQueue<AppendRequest>();

            shardLog.TryPost(new WalRecordCommitted(AppendRecord(0, oldTimestamp, "hello world")));
            await WaitForVisibleAsync(shardLog, 0);

            await using var evaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
                shardLog, ackIndex, IsMandatory: _ => true, GetMaxRetention: () => MaxRetention,
                ForwardToDeadLetterAsync: Forward(forwarded), IsAdmissionPaused: () => true, IsLeader: () => true, timeProvider));

            await AdvanceAndSettleAsync(timeProvider);

            forwarded.Should().BeEmpty("admission is paused — no forwarding/pruning may run");
            evaluator.SafeToPruneWatermark.Should().Be(0);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    static Func<AppendRequest, CancellationToken, Task<bool>> Forward(ConcurrentQueue<AppendRequest> forwarded) =>
        (request, _) => {
            forwarded.Enqueue(request);
            return Task.FromResult(true);
        };

    static ulong ToMicros(DateTimeOffset at) => (ulong)at.ToUnixTimeMilliseconds() * 1000;

    static WalRecord AppendRecord(ulong offset, ulong timestampMicros, string payload) =>
        new(WalRecordType.Append, RaftTerm: 0, RaftIndex: 0, offset, timestampMicros,
            "order-42"u8.ToArray(), [], Encoding.UTF8.GetBytes(payload));

    // The wire format an Ack record's payload always carries: [uint32 count][uint64 offsets...], little-endian.
    static WalRecord AckRecord(string consumerGroup, params ulong[] offsets) {
        var buffer = new byte[4 + offsets.Length * 8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(buffer, (uint)offsets.Length);
        for (var i = 0; i < offsets.Length; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(4 + i * 8, 8), offsets[i]);

        return new WalRecord(WalRecordType.Ack, RaftTerm: 0, RaftIndex: 0, LogicalOffset: 0, TimestampMicros: 42,
            Encoding.UTF8.GetBytes(consumerGroup), [], buffer);
    }

    static async Task<LogEntry> WaitForVisibleAsync(ShardLog log, ulong offset) {
        for (var i = 0; i < 500; i++) {
            if (log.TryGet(offset, out var handle))
                using (handle)
                    return handle.Entry;

            await Task.Delay(10);
        }

        throw new TimeoutException($"Offset {offset} never became visible.");
    }

    // Same leading+trailing real-time settle window AckCheckpointerTests/SizePruneWorkerTests use,
    // for the same underlying reason: Task.Run schedules the loop onto the thread pool asynchronously.
    static async Task AdvanceAndSettleAsync(FakeTimeProvider timeProvider) {
        await Task.Delay(20);
        timeProvider.Advance(TickInterval);
        await Task.Delay(50);
    }

    static string CreateTempDirectory() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }

    static async IAsyncEnumerable<WalRecord> EmptyCommittedSource([EnumeratorCancellation] CancellationToken cancellationToken) {
        await Task.CompletedTask;
        yield break;
    }
}
