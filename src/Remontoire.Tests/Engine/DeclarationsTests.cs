using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Sharding;

namespace Remontoire.Engine;

// The builder's declarations, applied while the host starts: streams first, then consumer groups,
// nothing proposed twice, and a conflicting declaration failing the start.
public class DeclarationsTests {
    static string NewDataDirectory() => Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    static async Task<IHost> StartAsync(string dataDirectory, Action<RemontoireBuilder>? configure = null) {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        var remontoire = builder.Services.AddRemontoire();
        remontoire.AddEmbedded(options => options.DataDirectory = dataDirectory);
        configure?.Invoke(remontoire);

        var host = builder.Build();
        try {
            await host.StartAsync();
        } catch {
            // The services that did start hold files open: stop them, or the directory can't be deleted.
            await StopAsync(host);
            throw;
        }

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
    public async Task Declared_streams_and_consumer_groups_exist_once_the_host_has_started() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory, remontoire => remontoire
                .AddStream("orders", options => options.MaxRetention = TimeSpan.FromDays(14))
                .AddConsumerGroup("orders", "billing"));
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();

            var description = (await admin.GetStreamAsync("orders"))!;
            description.MaxRetention.Should().Be(TimeSpan.FromDays(14));
            description.ConsumerGroups.Should().BeEquivalentTo([new ConsumerGroupDescription("billing", AckMode.Strict, true)]);

            await producer.PublishAsync("orders", "key-1", "one"u8.ToArray());
            var message = await FirstAsync(consumer, "orders", "billing");
            await consumer.AckAsync("orders", "billing", message.ShardId, message.Offset);
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_restart_with_the_same_declarations_proposes_nothing() {
        var dataDirectory = NewDataDirectory();
        IHost? host1 = null;
        IHost? host2 = null;
        try {
            host1 = await StartAsync(dataDirectory, remontoire => remontoire
                .AddStream("orders", options => options.MaxRetention = TimeSpan.FromDays(14))
                .AddConsumerGroup("orders", "billing"));
            var journal = host1.Services.GetRequiredService<MetaLogJournal>();
            var recordsAfterFirstStart = journal.Snapshot().Records.Count;
            await StopAsync(host1);
            host1 = null;

            host2 = await StartAsync(dataDirectory, remontoire => remontoire
                .AddStream("orders", options => options.MaxRetention = TimeSpan.FromDays(14))
                .AddConsumerGroup("orders", "billing"));

            var secondJournal = host2.Services.GetRequiredService<MetaLogJournal>();
            secondJournal.Snapshot().Records.Count.Should().Be(recordsAfterFirstStart, "a restart with unchanged declarations proposes nothing");
        } finally {
            if (host1 is not null)
                await StopAsync(host1);
            if (host2 is not null)
                await StopAsync(host2);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_changed_declaration_is_applied_at_the_next_start() {
        var dataDirectory = NewDataDirectory();
        IHost? host1 = null;
        IHost? host2 = null;
        try {
            host1 = await StartAsync(dataDirectory, remontoire => remontoire
                .AddStream("orders", options => options.MaxRetention = TimeSpan.FromDays(14))
                .AddConsumerGroup("orders", "billing"));
            await StopAsync(host1);
            host1 = null;

            host2 = await StartAsync(dataDirectory, remontoire => remontoire
                .AddStream("orders", options => options.MaxRetention = TimeSpan.FromDays(7))
                .AddConsumerGroup("orders", "billing", options => options.Mandatory = false));
            var description = (await host2.Services.GetRequiredService<IRemontoireAdmin>().GetStreamAsync("orders"))!;

            description.MaxRetention.Should().Be(TimeSpan.FromDays(7));
            description.ConsumerGroups.Should().BeEquivalentTo([new ConsumerGroupDescription("billing", AckMode.Strict, false)]);
        } finally {
            if (host1 is not null)
                await StopAsync(host1);
            if (host2 is not null)
                await StopAsync(host2);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_conflicting_declaration_fails_the_start() {
        var dataDirectory = NewDataDirectory();
        IHost? host1 = null;
        IHost? host2 = null;
        try {
            host1 = await StartAsync(dataDirectory);
            var node = host1.Services.GetRequiredService<RemontoireNode>();
            await node.ProposeMetaAsync([new CreateStream("orders", 4, RoutingAlgorithm.XxHash3V1)], proposedBy: null, CancellationToken.None);
            await StopAsync(host1);
            host1 = null;

            var act = () => StartAsync(dataDirectory, remontoire => remontoire.AddStream("orders"));

            await act.Should().ThrowAsync<RemontoireConflictException>();
        } finally {
            if (host1 is not null)
                await StopAsync(host1);
            if (host2 is not null)
                await StopAsync(host2);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }
}
