using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Sharding;

namespace Remontoire.Engine;

// The administration of a node, exercised in-process on an embedded host: declaring streams and
// consumer groups, applying changed options, and describing the result.
public class NodeAdminTests {
    static string NewDataDirectory() => Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    static async Task<IHost> StartAsync(string dataDirectory) {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddRemontoire().AddEmbedded(options => options.DataDirectory = dataDirectory);

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    static async Task StopAsync(IHost host) {
        await host.StopAsync();
        host.Dispose();
    }

    static async Task<RemontoireMessage> FirstAsync(IRemontoireConsumer consumer, string streamName, string consumerGroup) {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var message in consumer.ConsumeAsync(streamName, consumerGroup, cts.Token))
            return message;

        throw new InvalidOperationException($"No message arrived on stream '{streamName}' for group '{consumerGroup}'.");
    }

    [Fact]
    public async Task EnsureStreamAsync_creates_a_stream_that_accepts_messages() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing");

            (await producer.PublishAsync("orders", "key-1", "one"u8.ToArray())).Offset.Should().Be(0);

            var message = await FirstAsync(consumer, "orders", "billing");
            message.PartitionKey.Should().Be("key-1");
            await consumer.AckAsync("orders", "billing", message.ShardId, message.Offset);
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task EnsureStreamAsync_applies_changed_options() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var journal = host.Services.GetRequiredService<MetaLogJournal>();

            await admin.EnsureStreamAsync("orders");
            (await admin.GetStreamAsync("orders"))!.MaxRetention.Should().Be(TimeSpan.FromDays(30), "the defaults apply first");

            var changed = new StreamOptions {
                MaxRetention = TimeSpan.FromDays(7),
                MaxSizeBytesPerVirtualShard = 1024 * 1024,
                CheckpointInterval = TimeSpan.FromSeconds(5),
                CheckpointOffsetCount = 100,
            };
            await admin.EnsureStreamAsync("orders", changed);
            var description = (await admin.GetStreamAsync("orders"))!;
            description.MaxRetention.Should().Be(TimeSpan.FromDays(7));
            description.MaxSizeBytesPerVirtualShard.Should().Be(1024 * 1024);
            description.CheckpointInterval.Should().Be(TimeSpan.FromSeconds(5));
            description.CheckpointOffsetCount.Should().Be(100);

            var recordsAfterSecond = journal.Snapshot().Records.Count;
            await admin.EnsureStreamAsync("orders", changed);
            journal.Snapshot().Records.Count.Should().Be(recordsAfterSecond, "a declaration that matches the current state proposes nothing");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task EnsureStreamAsync_fails_with_a_conflict_for_a_stream_with_another_virtual_shard_count() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var node = host.Services.GetRequiredService<RemontoireNode>();
            await node.ProposeMetaAsync([new CreateStream("orders", 4, RoutingAlgorithm.XxHash3V1)], proposedBy: null, CancellationToken.None);

            var act = () => admin.EnsureStreamAsync("orders");

            (await act.Should().ThrowAsync<RemontoireConflictException>()).Which.Message.Should().Contain("orders").And.Contain("4");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task EnsureStreamAsync_rejects_invalid_options(int caseIndex) {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();

            Func<Task> act = caseIndex switch {
                0 => () => admin.EnsureStreamAsync("orders", new StreamOptions { MaxRetention = TimeSpan.Zero }),
                1 => () => admin.EnsureStreamAsync("orders", new StreamOptions { MaxSizeBytesPerVirtualShard = 0 }),
                2 => () => admin.EnsureStreamAsync("orders", new StreamOptions { CheckpointInterval = TimeSpan.Zero }),
                _ => () => admin.EnsureStreamAsync("orders", new StreamOptions { CheckpointOffsetCount = 0 }),
            };

            await act.Should().ThrowAsync<ArgumentException>();
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task EnsureConsumerGroupAsync_declares_and_later_updates_the_group() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            await admin.EnsureStreamAsync("orders");

            await admin.EnsureConsumerGroupAsync("orders", "billing");
            (await admin.GetStreamAsync("orders"))!.ConsumerGroups.Should().BeEquivalentTo(
                [new ConsumerGroupDescription("billing", AckMode.Strict, true)]);

            await admin.EnsureConsumerGroupAsync("orders", "billing", new ConsumerGroupOptions { AckMode = AckMode.Checkpoint, Mandatory = false });
            (await admin.GetStreamAsync("orders"))!.ConsumerGroups.Should().BeEquivalentTo(
                [new ConsumerGroupDescription("billing", AckMode.Checkpoint, false)]);
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("-billing")]
    public async Task EnsureConsumerGroupAsync_rejects_an_invalid_name(string consumerGroup) {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();

            var act = () => admin.EnsureConsumerGroupAsync("orders", consumerGroup);

            await act.Should().ThrowAsync<ArgumentException>();
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task EnsureConsumerGroupAsync_accepts_a_stream_that_does_not_exist_yet() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();

            await admin.EnsureConsumerGroupAsync("later", "billing");

            await admin.EnsureStreamAsync("later");
            var description = (await admin.GetStreamAsync("later"))!;
            description.ConsumerGroups.Should().BeEquivalentTo([new ConsumerGroupDescription("billing", AckMode.Strict, true)]);
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task ListStreamsAsync_and_GetStreamAsync_describe_the_declarations() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing");
            await admin.EnsureStreamAsync("payments");
            await admin.EnsureConsumerGroupAsync("payments", "payroll");

            var streams = await admin.ListStreamsAsync();

            streams.Select(stream => stream.Name).Should().Equal("orders", "payments");
            (await admin.GetStreamAsync("orders"))!.ConsumerGroups.Select(group => group.Name).Should().Equal("billing");
            (await admin.GetStreamAsync("unknown")).Should().BeNull();
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }
}
