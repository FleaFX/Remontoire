using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Remontoire.Storage;

public class Benchmarks {
#if RELEASE
    [Fact]
#else
    [Fact(Skip = "To run the benchmarks, build in Release config.")]
#endif
    public void ShardLogAppendThroughput() => Run<ShardLogAppendBenchmarks>();

#if RELEASE
    [Fact]
#else
    [Fact(Skip = "To run the benchmarks, build in Release config.")]
#endif
    public void MemTableLookup() => Run<MemTableLookupBenchmarks>();

#if RELEASE
    [Fact]
#else
    [Fact(Skip = "To run the benchmarks, build in Release config.")]
#endif
    public void SstSegmentLookup() => Run<SstSegmentLookupBenchmarks>();

    // In-process toolchain: MemTable/SstSegment are internal types. InternalsVisibleTo grants
    // Remontoire.Tests access, not the separate runner assembly BenchmarkDotNet normally
    // generates for out-of-process execution — running in-process avoids that assembly
    // boundary entirely instead of exposing internals just for benchmarking.
    static void Run<T>() =>
        BenchmarkRunner.Run<T>(ManualConfig.Create(DefaultConfig.Instance).AddJob(Job.Default.WithToolchain(InProcessEmitToolchain.Instance)));
}

/// <summary>
/// Append throughput under concurrent load — the scenario group commit exists for; a
/// single-threaded benchmark would never show its benefit.
/// </summary>
[MemoryDiagnoser]
public class ShardLogAppendBenchmarks {
    string _directory = null!;
    ShardLog _log = null!;
    AppendRequest _request;

    [Params(1, 10, 50)]
    public int ConcurrentAppends { get; set; }

    [GlobalSetup]
    public async Task SetupAsync() {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);
        _log = await ShardLog.OpenAsync(_directory);
        _request = new AppendRequest(Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes("hello world"));
    }

    [Benchmark]
    public Task AppendAsync() {
        var tasks = new Task[ConcurrentAppends];
        for (var i = 0; i < ConcurrentAppends; i++)
            tasks[i] = _log.AppendAsync(_request).AsTask();
        return Task.WhenAll(tasks);
    }

    [GlobalCleanup]
    public async Task CleanupAsync() {
        await _log.DisposeAsync();
        Directory.Delete(_directory, recursive: true);
    }
}

/// <summary>
/// Point-lookup latency in a MemTable of varying size.
/// </summary>
[MemoryDiagnoser]
public class MemTableLookupBenchmarks {
    MemTable _memTable = null!;
    ulong _middleOffset;

    [Params(1_000, 100_000)]
    public int EntryCount { get; set; }

    [GlobalSetup]
    public void Setup() {
        _memTable = new MemTable();
        var partitionKey = Encoding.UTF8.GetBytes("order-42");
        var payload = Encoding.UTF8.GetBytes("hello world");

        for (ulong i = 0; i < (ulong)EntryCount; i++)
            _memTable.Append(new LogEntry(i, TimestampMicros: 42, partitionKey, [], payload));

        _middleOffset = (ulong)EntryCount / 2;
    }

    [Benchmark]
    public bool TryGet() => _memTable.TryGet(_middleOffset, out _);

    [GlobalCleanup]
    public void Cleanup() => _memTable.Dispose();
}

/// <summary>
/// Sparse-index point-lookup latency in an SST segment of varying size.
/// </summary>
[MemoryDiagnoser]
public class SstSegmentLookupBenchmarks {
    string _path = null!;
    SstSegment _segment = null!;
    ulong _middleOffset;

    [Params(1_000, 100_000)]
    public int EntryCount { get; set; }

    [GlobalSetup]
    public async Task SetupAsync() {
        _path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var partitionKey = Encoding.UTF8.GetBytes("order-42");
        var payload = Encoding.UTF8.GetBytes("hello world");

        var entries = Enumerable.Range(0, EntryCount)
            .Select(i => new LogEntry((ulong)i, TimestampMicros: 42, partitionKey, [], payload));
        await SstWriter.WriteAsync(_path, entries);

        _segment = await SstSegment.OpenAsync(_path);
        _middleOffset = (ulong)EntryCount / 2;
    }

    [Benchmark]
    public bool TryGet() {
        var found = _segment.TryGet(_middleOffset, out var result);
        if (found)
            result.Dispose();
        return found;
    }

    [GlobalCleanup]
    public void Cleanup() {
        _segment.Dispose();
        File.Delete(_path);
    }
}
