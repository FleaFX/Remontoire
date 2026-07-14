using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using FluentAssertions;
using Remontoire.Server;
using Remontoire.Storage;

namespace Remontoire.Messaging;

public class AckIndexApplierTests {
    [Fact]
    public async Task Forwards_an_Ack_record_from_ShardLog_into_the_AckIndex() {
        var directory = CreateTempDirectory();
        try {
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            await using var applier = new AckIndexApplier(shardLog, ackIndex);

            shardLog.TryPost(new WalRecordCommitted(AckRecord("group-1", 0)));

            (await WaitUntilAsync(() => ackIndex.GetOrCreate("group-1").IsAcked(0))).Should().BeTrue();
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Ignores_Append_records_forwarded_alongside_Acks() {
        var directory = CreateTempDirectory();
        try {
            await using var shardLog = await ShardLog.OpenAsync(directory, EmptyCommittedSource);
            var ackIndex = new AckIndex();
            await using var applier = new AckIndexApplier(shardLog, ackIndex);

            shardLog.TryPost(new WalRecordCommitted(new WalRecord(WalRecordType.Append, RaftTerm: 0, RaftIndex: 0, LogicalOffset: 0,
                TimestampMicros: 42, "order-42"u8.ToArray(), [], "hello world"u8.ToArray())));
            shardLog.TryPost(new WalRecordCommitted(AckRecord("group-1", 0)));

            (await WaitUntilAsync(() => ackIndex.GetOrCreate("group-1").IsAcked(0))).Should().BeTrue();
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    static WalRecord AckRecord(string consumerGroup, params ulong[] offsets) =>
        new(WalRecordType.Ack, RaftTerm: 0, RaftIndex: 0, LogicalOffset: 0, TimestampMicros: 42,
            Encoding.UTF8.GetBytes(consumerGroup), [], EncodeOffsets(offsets));

    static byte[] EncodeOffsets(params ulong[] offsets) {
        var buffer = new byte[4 + offsets.Length * 8];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer, (uint)offsets.Length);

        for (var i = 0; i < offsets.Length; i++)
            BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(4 + i * 8, 8), offsets[i]);

        return buffer;
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

    static Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null) =>
        ConditionPoller.WaitUntilAsync(condition, timeout ?? TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(5));
}
