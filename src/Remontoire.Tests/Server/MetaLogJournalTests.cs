using FluentAssertions;

namespace Remontoire.Server;

public class MetaLogJournalTests {
    public class Snapshot {
        [Fact]
        public void Returns_version_zero_and_no_records_when_empty() {
            var journal = new MetaLogJournal();

            var (version, records) = journal.Snapshot();

            version.Should().Be(0);
            records.Should().BeEmpty();
        }

        [Fact]
        public void Returns_every_appended_record_in_order_with_the_latest_version() {
            var journal = new MetaLogJournal();
            journal.Append(1, "first"u8.ToArray());
            journal.Append(2, "second"u8.ToArray());

            var (version, records) = journal.Snapshot();

            version.Should().Be(2);
            records.Select(r => r.Version).Should().Equal(1UL, 2UL);
            records.Select(r => System.Text.Encoding.UTF8.GetString(r.Payload)).Should().Equal("first", "second");
        }
    }

    public class WatchAsync {
        [Fact]
        public async Task Yields_records_already_appended_before_the_watch_started() {
            var journal = new MetaLogJournal();
            journal.Append(1, "first"u8.ToArray());
            journal.Append(2, "second"u8.ToArray());

            using var cts = new CancellationTokenSource();
            var seen = new List<ulong>();
            await foreach (var (version, _) in journal.WatchAsync(0, cts.Token)) {
                seen.Add(version);
                if (seen.Count == 2)
                    break;
            }

            seen.Should().Equal(1UL, 2UL);
        }

        [Fact]
        public async Task Skips_records_at_or_before_the_requested_from_version() {
            var journal = new MetaLogJournal();
            journal.Append(1, "first"u8.ToArray());
            journal.Append(2, "second"u8.ToArray());
            journal.Append(3, "third"u8.ToArray());

            var seen = new List<ulong>();
            await foreach (var (version, _) in journal.WatchAsync(2, CancellationToken.None)) {
                seen.Add(version);
                if (seen.Count == 1)
                    break;
            }

            seen.Should().Equal(3UL);
        }

        [Fact]
        public async Task Yields_a_record_appended_after_the_watch_already_started() {
            var journal = new MetaLogJournal();
            using var cts = new CancellationTokenSource();

            var enumerator = journal.WatchAsync(0, cts.Token).GetAsyncEnumerator(cts.Token);
            var moveNext = enumerator.MoveNextAsync();

            moveNext.IsCompleted.Should().BeFalse("nothing has been appended yet");
            journal.Append(1, "late"u8.ToArray());

            (await moveNext).Should().BeTrue();
            enumerator.Current.Version.Should().Be(1);
        }
    }
}
