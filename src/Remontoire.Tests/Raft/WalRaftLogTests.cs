using FluentAssertions;
using Remontoire.Storage;

namespace Remontoire.Raft;

public class WalRaftLogTests {
    [Fact]
    public async Task Starts_empty_at_the_snapshot_base() {
        var path = TempWalPath();
        try {
            await using var log = await WalRaftLog.OpenAsync(path);

            log.LastIndex.Should().Be(0);
            log.LastTerm.Should().Be(0);
            (await ToListAsync(log.ReadFromAsync(1))).Should().BeEmpty();
        } finally {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task AppendAsync_then_ReadFromAsync_yields_the_appended_entries_in_order() {
        var path = TempWalPath();
        try {
            await using var log = await WalRaftLog.OpenAsync(path);

            await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2), Entry(term: 1, index: 3)]);

            log.LastIndex.Should().Be(3);
            log.LastTerm.Should().Be(1);
            (await ToListAsync(log.ReadFromAsync(2))).Select(r => r.RaftIndex).Should().Equal(2ul, 3ul);
        } finally {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task GetTermAtAsync_returns_the_term_of_the_entry_at_that_index() {
        var path = TempWalPath();
        try {
            await using var log = await WalRaftLog.OpenAsync(path);

            await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 2, index: 2)]);

            (await log.GetTermAtAsync(1)).Should().Be(1);
            (await log.GetTermAtAsync(2)).Should().Be(2);
        } finally {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task TruncateFromAsync_removes_the_entry_and_everything_after_it() {
        var path = TempWalPath();
        try {
            await using var log = await WalRaftLog.OpenAsync(path);

            await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2), Entry(term: 2, index: 3)]);
            await log.TruncateFromAsync(2);

            log.LastIndex.Should().Be(1);
            log.LastTerm.Should().Be(1);
            (await ToListAsync(log.ReadFromAsync(1))).Select(r => r.RaftIndex).Should().Equal(1ul);
        } finally {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task TruncateFromAsync_lets_a_later_append_reuse_the_freed_index() {
        var path = TempWalPath();
        try {
            await using var log = await WalRaftLog.OpenAsync(path);

            await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2)]);
            await log.TruncateFromAsync(2);
            await log.AppendAsync([Entry(term: 2, index: 2)]);

            log.LastIndex.Should().Be(2);
            log.LastTerm.Should().Be(2);
            (await log.GetTermAtAsync(2)).Should().Be(2);
        } finally {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task OpenAsync_recovers_the_position_index_from_an_existing_file() {
        var path = TempWalPath();
        try {
            await using (var log = await WalRaftLog.OpenAsync(path))
                await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2), Entry(term: 2, index: 3)]);

            await using var reopened = await WalRaftLog.OpenAsync(path);

            reopened.LastIndex.Should().Be(3);
            reopened.LastTerm.Should().Be(2);
            (await reopened.GetTermAtAsync(2)).Should().Be(1);
            (await ToListAsync(reopened.ReadFromAsync(2))).Select(r => r.RaftIndex).Should().Equal(2ul, 3ul);
        } finally {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task OpenAsync_never_recovers_a_truncated_away_suffix() {
        var path = TempWalPath();
        try {
            await using (var log = await WalRaftLog.OpenAsync(path)) {
                await log.AppendAsync([Entry(term: 1, index: 1), Entry(term: 1, index: 2)]);
                await log.TruncateFromAsync(2);
            }

            await using var reopened = await WalRaftLog.OpenAsync(path);

            reopened.LastIndex.Should().Be(1);
            (await ToListAsync(reopened.ReadFromAsync(1))).Select(r => r.RaftIndex).Should().Equal(1ul);
        } finally {
            File.Delete(path);
        }
    }

    static string TempWalPath() => Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    static WalRecord Entry(ulong term, ulong index) =>
        new(WalRecordType.Append, RaftTerm: term, RaftIndex: index, LogicalOffset: index - 1, TimestampMicros: 42,
            PartitionKey: "order-42"u8.ToArray(), Headers: [], Payload: "hello world"u8.ToArray());

    static async Task<List<WalRecord>> ToListAsync(IAsyncEnumerable<WalRecord> records) {
        var list = new List<WalRecord>();
        await foreach (var record in records)
            list.Add(record);
        return list;
    }
}
