using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Remontoire.Storage.Compaction;

namespace Remontoire.Storage;

public class SizePruneWorkerTests {
    static readonly TimeSpan TickInterval = TimeSpan.FromMinutes(5);

    [Fact]
    public async Task Posts_SizePruneCompleted_once_over_budget() {
        var directory = CreateTempDirectory();
        try {
            var path = await WriteSegmentAsync(directory, SampleEntries(0, 5));
            var (mailbox, posted, timeProvider) = Compose();
            using var cts = new CancellationTokenSource();
            var worker = new SizePruneWorker(directory, getMaxTotalBytes: () => 0, isAdmissionPaused: null, mailbox, timeProvider);
            var run = worker.RunAsync(cts.Token);

            await AdvanceAndSettleAsync(timeProvider);

            posted.Should().ContainSingle();
            posted.Single().DeletedPaths.Should().Equal(path);
            File.Exists(path).Should().BeFalse();

            await StopAsync(cts, run);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Posts_nothing_when_already_under_budget() {
        var directory = CreateTempDirectory();
        try {
            var path = await WriteSegmentAsync(directory, SampleEntries(0, 5));
            var budget = new FileInfo(path).Length + 1;
            var (mailbox, posted, timeProvider) = Compose();
            using var cts = new CancellationTokenSource();
            var worker = new SizePruneWorker(directory, getMaxTotalBytes: () => budget, isAdmissionPaused: null, mailbox, timeProvider);
            var run = worker.RunAsync(cts.Token);

            await AdvanceAndSettleAsync(timeProvider);

            posted.Should().BeEmpty();
            File.Exists(path).Should().BeTrue();

            await StopAsync(cts, run);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Never_prunes_while_the_ceiling_delegate_returns_null_and_recovers_once_it_resolves() {
        var directory = CreateTempDirectory();
        try {
            var path = await WriteSegmentAsync(directory, SampleEntries(0, 5));
            var (mailbox, posted, timeProvider) = Compose();
            using var cts = new CancellationTokenSource();
            long? ceiling = null; // simulates a not-yet-resolved stream assignment at startup
            var worker = new SizePruneWorker(directory, getMaxTotalBytes: () => ceiling, isAdmissionPaused: null, mailbox, timeProvider);
            var run = worker.RunAsync(cts.Token);

            await AdvanceAndSettleAsync(timeProvider);
            posted.Should().BeEmpty("no ceiling is known yet — this tick must skip, not disable pruning forever");
            File.Exists(path).Should().BeTrue();

            ceiling = 0; // the ceiling becomes known on a later tick
            await AdvanceAndSettleAsync(timeProvider);

            posted.Should().ContainSingle("the same worker must resume pruning once the ceiling resolves, without needing to be restarted");
            File.Exists(path).Should().BeFalse();

            await StopAsync(cts, run);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Never_faults_the_loop_when_cancellation_races_the_prune_itself() {
        // Unlike AckCheckpointer/RetentionEvaluator, this loop had no outer OperationCanceledException
        // catch around the whole while(true) — only around the idle Task.Delay. A cancellation that
        // lands while the prune itself is in flight (not just while idling) must still shut the
        // loop down cleanly rather than faulting RunAsync's own Task.
        var directory = CreateTempDirectory();
        try {
            var path = await WriteSegmentAsync(directory, SampleEntries(0, 5));
            var (mailbox, posted, timeProvider) = Compose();
            using var cts = new CancellationTokenSource();
            var worker = new SizePruneWorker(directory, getMaxTotalBytes: () => { cts.Cancel(); return 0; }, isAdmissionPaused: null, mailbox, timeProvider);
            var run = worker.RunAsync(cts.Token);

            await AdvanceAndSettleAsync(timeProvider);

            var act = async () => await run;
            await act.Should().NotThrowAsync("cancellation racing the prune's own work must shut the loop down cleanly, not fault it");
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Counts_a_persistently_failing_tick_instead_of_leaving_it_unobservable() {
        // A persistently broken emergency-pruning path (a corrupt/locked segment, here simulated
        // via a directory that no longer exists) must be observable, not silently retried forever
        // with zero signal — exactly the scenario this worker exists to guard against (a full disk).
        var missingDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()); // never created
        var (mailbox, posted, timeProvider) = Compose();
        using var cts = new CancellationTokenSource();
        var worker = new SizePruneWorker(missingDirectory, getMaxTotalBytes: () => 0, isAdmissionPaused: null, mailbox, timeProvider);
        var run = worker.RunAsync(cts.Token);

        await AdvanceAndSettleAsync(timeProvider);
        await AdvanceAndSettleAsync(timeProvider);

        worker.FailedTicksTotal.Should().Be(2, "each failing tick must be counted, not silently swallowed");
        posted.Should().BeEmpty();

        await StopAsync(cts, run);
    }

    [Fact]
    public async Task Never_ticks_while_admission_is_paused() {
        var directory = CreateTempDirectory();
        try {
            var path = await WriteSegmentAsync(directory, SampleEntries(0, 5));
            var (mailbox, posted, timeProvider) = Compose();
            using var cts = new CancellationTokenSource();
            var worker = new SizePruneWorker(directory, getMaxTotalBytes: () => 0, isAdmissionPaused: () => true, mailbox, timeProvider);
            var run = worker.RunAsync(cts.Token);

            await AdvanceAndSettleAsync(timeProvider);

            posted.Should().BeEmpty("admission is paused — no pruning may run, even though the directory is over budget");
            File.Exists(path).Should().BeTrue();

            await StopAsync(cts, run);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    static (ChannelWriter<ShardLogMessage> Mailbox, List<SizePruneCompleted> Posted, FakeTimeProvider TimeProvider) Compose() {
        var channel = Channel.CreateUnbounded<ShardLogMessage>();
        var posted = new List<SizePruneCompleted>();
        _ = ConsumeAsync(channel.Reader, posted);
        return (channel.Writer, posted, new FakeTimeProvider());
    }

    static async Task ConsumeAsync(ChannelReader<ShardLogMessage> reader, List<SizePruneCompleted> posted) {
        await foreach (var message in reader.ReadAllAsync()) {
            if (message is SizePruneCompleted completed)
                posted.Add(completed);
        }
    }

    static async Task StopAsync(CancellationTokenSource cts, Task run) {
        await cts.CancelAsync();
        await run;
    }

    // Same leading+trailing real-time settle window AckCheckpointerTests uses, for the same
    // underlying reason: Task.Run schedules the worker loop onto the thread pool asynchronously.
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

    static async Task<string> WriteSegmentAsync(string directory, IEnumerable<LogEntry> entries) {
        var materialized = entries.ToList();
        var path = Path.Combine(directory, $"segment-{materialized[0].LogicalOffset:D20}.sst");
        await SstWriter.WriteAsync(path, materialized);
        return path;
    }

    static IEnumerable<LogEntry> SampleEntries(ulong startOffset, int count) {
        for (var i = 0; i < count; i++) {
            var offset = startOffset + (ulong)i;
            yield return new LogEntry(offset, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [],
                Encoding.UTF8.GetBytes($"hello world-{offset}"));
        }
    }
}
