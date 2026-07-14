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
            journal.Append(1, "first"u8.ToArray(), 0, []);
            journal.Append(2, "second"u8.ToArray(), 0, []);

            var (version, records) = journal.Snapshot();

            version.Should().Be(2);
            records.Select(r => r.Version).Should().Equal(1UL, 2UL);
            records.Select(r => System.Text.Encoding.UTF8.GetString(r.Payload)).Should().Equal("first", "second");
        }
    }

    public class WatchAsync {
        [Fact]
        public async Task Yields_the_very_first_record_ever_appended_at_version_zero() {
            // Real production versions are Raft LogicalOffsets — 0-based, so the FIRST record a
            // meta-group ever commits genuinely has version 0. A watcher that starts up before
            // anything has been proposed yet (a real, common startup race) gets an empty Snapshot
            // reporting Version 0 as its own "nothing yet" sentinel — indistinguishable from "I've
            // already seen version 0" unless WatchAsync(0, ...) is treated as an INCLUSIVE lower
            // bound. Confirmed via a real CI failure: version 0 (CreateStream) was silently, always
            // skipped this way while every later version applied normally.
            var journal = new MetaLogJournal();
            journal.Append(0, "first"u8.ToArray(), 0, []);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2)); // must not hang if this regresses
            var seen = new List<ulong>();
            await foreach (var (version, _, _, _) in journal.WatchAsync(0, cts.Token)) {
                seen.Add(version);
                break;
            }

            seen.Should().Equal(0UL);
        }

        [Fact]
        public async Task Yields_records_already_appended_before_the_watch_started() {
            var journal = new MetaLogJournal();
            journal.Append(1, "first"u8.ToArray(), 0, []);
            journal.Append(2, "second"u8.ToArray(), 0, []);

            using var cts = new CancellationTokenSource();
            var seen = new List<ulong>();
            await foreach (var (version, _, _, _) in journal.WatchAsync(0, cts.Token)) {
                seen.Add(version);
                if (seen.Count == 2)
                    break;
            }

            seen.Should().Equal(1UL, 2UL);
        }

        [Fact]
        public async Task Skips_records_strictly_before_the_requested_from_version_but_includes_it() {
            var journal = new MetaLogJournal();
            journal.Append(1, "first"u8.ToArray(), 0, []);
            journal.Append(2, "second"u8.ToArray(), 0, []);
            journal.Append(3, "third"u8.ToArray(), 0, []);

            var seen = new List<ulong>();
            await foreach (var (version, _, _, _) in journal.WatchAsync(2, CancellationToken.None)) {
                seen.Add(version);
                if (seen.Count == 2)
                    break;
            }

            seen.Should().Equal([2UL, 3UL], "fromVersion is inclusive — version 2 itself must still come through, only 1 is skipped");
        }

        [Fact]
        public async Task Yields_a_record_appended_after_the_watch_already_started() {
            var journal = new MetaLogJournal();
            using var cts = new CancellationTokenSource();

            var enumerator = journal.WatchAsync(0, cts.Token).GetAsyncEnumerator(cts.Token);
            var moveNext = enumerator.MoveNextAsync();

            moveNext.IsCompleted.Should().BeFalse("nothing has been appended yet");
            journal.Append(1, "late"u8.ToArray(), 0, []);

            (await moveNext).Should().BeTrue();
            enumerator.Current.Version.Should().Be(1);
        }
    }
}
