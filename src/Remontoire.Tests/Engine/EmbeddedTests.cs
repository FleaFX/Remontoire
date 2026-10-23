using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;

namespace Remontoire.Engine;

// The embedded engine's acceptance criterion — publish, consume and ack in one process, with
// everything surviving a restart — plus the semantics it must share with a cluster by
// construction: dead-lettering of a stuck mandatory group, checkpoint-mode acks and the
// client-address registration a remote client depends on.
public class EmbeddedTests {
    static string NewDataDirectory() => Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    static async Task<IHost> StartAsync(string dataDirectory, Action<RemontoireEngineOptions>? configure = null) {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddRemontoire().AddEmbedded(options => {
            options.DataDirectory = dataDirectory;
            configure?.Invoke(options);
        });

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    static async Task StopAsync(IHost host) {
        await host.StopAsync();
        host.Dispose();
    }

    static async Task ProposeMetaAsync(IHost host, MetaLogRecord record) {
        var registry = host.Services.GetRequiredService<RaftReplicaRegistry>();
        registry.TryGet(RemontoireNode.MetaGroupId, out var metaReplica).Should().BeTrue();
        await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [], MetaLogRecord.Encode(record)));
    }

    static async Task<RemontoireMessage> FirstAsync(IRemontoireConsumer consumer, string streamName, string consumerGroup) {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var message in consumer.ConsumeAsync(streamName, consumerGroup, cts.Token))
            return message;

        throw new InvalidOperationException($"No message arrived on stream '{streamName}' for group '{consumerGroup}'.");
    }

    [Fact]
    public async Task Publishes_consumes_and_acks_in_process_and_keeps_everything_after_a_restart() {
        var dataDirectory = NewDataDirectory();
        IHost? host1 = null;
        IHost? host2 = null;
        try {
            host1 = await StartAsync(dataDirectory);
            var admin = host1.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host1.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host1.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing");

            (await producer.PublishAsync("orders", "key-1", "one"u8.ToArray())).Offset.Should().Be(0);
            (await producer.PublishAsync("orders", "key-2", "two"u8.ToArray())).Offset.Should().Be(1);

            var message = await FirstAsync(consumer, "orders", "billing");
            message.Offset.Should().Be(0);
            message.PartitionKey.Should().Be("key-1");
            message.Payload.ToArray().Should().Equal("one"u8.ToArray());

            await consumer.AckAsync("orders", "billing", message.ShardId, message.Offset);
            await StopAsync(host1);
            host1 = null;

            host2 = await StartAsync(dataDirectory);
            var secondProducer = host2.Services.GetRequiredService<IRemontoireProducer>();
            var secondConsumer = host2.Services.GetRequiredService<IRemontoireConsumer>();

            var second = await FirstAsync(secondConsumer, "orders", "billing");
            second.Offset.Should().Be(1);
            second.Payload.ToArray().Should().Equal("two"u8.ToArray());

            (await secondProducer.PublishAsync("orders", "key-3", "three"u8.ToArray())).Offset.Should().Be(2);
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
    public async Task EnsureStreamAsync_does_nothing_for_an_existing_stream() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var journal = host.Services.GetRequiredService<MetaLogJournal>();

            await admin.EnsureStreamAsync("orders");
            var recordsAfterFirst = journal.Snapshot().Records.Count;

            await admin.EnsureStreamAsync("orders");

            (await producer.PublishAsync("orders", "key-1", "one"u8.ToArray())).Offset.Should().Be(0);
            host.Services.GetRequiredService<ShardAssignmentTable>().EnumerateAssignments().Should().HaveCount(1);
            journal.Snapshot().Records.Count.Should().Be(recordsAfterFirst, "a declaration that matches the current state proposes nothing");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("../orders")]
    [InlineData("__meta__")]
    [InlineData("orders/eu")]
    [InlineData(".hidden")]
    public async Task EnsureStreamAsync_rejects_a_name_that_is_not_a_safe_directory_name(string streamName) {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();

            var act = () => admin.EnsureStreamAsync(streamName);

            (await act.Should().ThrowAsync<ArgumentException>()).And.ParamName.Should().Be("streamName");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task PublishAsync_throws_for_a_stream_that_does_not_exist() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();

            var act = () => producer.PublishAsync("missing", "key", "payload"u8.ToArray());

            await act.Should().ThrowAsync<ArgumentException>();
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_stuck_mandatory_group_forces_a_dead_letter_forward_into_the_dead_letter_stream() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory, options => options.RetentionTickInterval = TimeSpan.FromMilliseconds(200));
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            var table = host.Services.GetRequiredService<ShardAssignmentTable>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureStreamAsync("orders.__deadletter__");
            await admin.EnsureConsumerGroupAsync("orders", "mandatory-group");
            await admin.EnsureConsumerGroupAsync("orders", "best-effort-group", new ConsumerGroupOptions { Mandatory = false });
            await admin.EnsureConsumerGroupAsync("orders.__deadletter__", "dead-letter-reader");

            await ProposeMetaAsync(host, new SetStreamRetentionPolicy("orders",
                AuditRetention: TimeSpan.Zero, MaxRetention: TimeSpan.FromMilliseconds(100), MaxSizeBytesPerVirtualShard: null));

            (await ConditionPoller.WaitUntilAsync(() => table.GetRetentionPolicy("orders").MaxRetention == TimeSpan.FromMilliseconds(100),
                TimeSpan.FromSeconds(2))).Should().BeTrue();

            await producer.PublishAsync("orders", "key-1", "hello"u8.ToArray());

            _ = await FirstAsync(consumer, "orders", "mandatory-group");
            _ = await FirstAsync(consumer, "orders", "best-effort-group");

            var deadLettered = await FirstAsync(consumer, "orders.__deadletter__", "dead-letter-reader");
            deadLettered.PartitionKey.Should().Be("key-1");
            deadLettered.Payload.ToArray().Should().Equal("hello"u8.ToArray());
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_checkpoint_mode_ack_survives_a_restart() {
        var dataDirectory = NewDataDirectory();
        IHost? host1 = null;
        IHost? host2 = null;
        try {
            host1 = await StartAsync(dataDirectory);
            var admin = host1.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host1.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host1.Services.GetRequiredService<IRemontoireConsumer>();
            var messagingRegistry = host1.Services.GetRequiredService<MessagingGroupRegistry>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing", new ConsumerGroupOptions { AckMode = AckMode.Checkpoint });

            await ProposeMetaAsync(host1, new SetStreamCheckpointInterval("orders", Interval: null, OffsetCount: 1));

            await producer.PublishAsync("orders", "key-1", "one"u8.ToArray());
            await producer.PublishAsync("orders", "key-2", "two"u8.ToArray());

            var message = await FirstAsync(consumer, "orders", "billing");
            message.Offset.Should().Be(0);
            await consumer.AckAsync("orders", "billing", message.ShardId, message.Offset);

            (await ConditionPoller.WaitUntilAsync(() =>
                    messagingRegistry.TryGet("orders", out var messaging) && messaging.AckIndex.GetOrCreate("billing").CommittedWatermark == 1,
                TimeSpan.FromSeconds(10))).Should().BeTrue();

            await StopAsync(host1);
            host1 = null;

            host2 = await StartAsync(dataDirectory);
            var secondConsumer = host2.Services.GetRequiredService<IRemontoireConsumer>();

            var second = await FirstAsync(secondConsumer, "orders", "billing");
            second.Offset.Should().Be(1);
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
    public async Task A_restart_with_another_client_address_registers_that_address_for_every_group() {
        var dataDirectory = NewDataDirectory();
        IHost? host1 = null;
        IHost? host2 = null;
        try {
            host1 = await StartAsync(dataDirectory, options => options.ClientAddress = new Uri("http://127.0.0.1:1"));
            var admin = host1.Services.GetRequiredService<IRemontoireAdmin>();
            await admin.EnsureStreamAsync("orders");
            await StopAsync(host1);
            host1 = null;

            host2 = await StartAsync(dataDirectory, options => options.ClientAddress = new Uri("http://127.0.0.1:2"));
            var table = host2.Services.GetRequiredService<ShardAssignmentTable>();

            (await ConditionPoller.WaitUntilAsync(() => table.TryGetGroup("orders", out var group)
                        && group.Members is [var only] && only.Address == new Uri("http://127.0.0.1:2"),
                TimeSpan.FromSeconds(5))).Should().BeTrue();
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
    public async Task ConsumeAsync_for_an_undeclared_consumer_group_throws() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await using var enumerator = consumer.ConsumeAsync("orders", "undeclared-group", cts.Token).GetAsyncEnumerator(cts.Token);

            var act = () => enumerator.MoveNextAsync().AsTask();

            (await act.Should().ThrowAsync<ArgumentException>()).Which.Message.Should().Contain("undeclared-group");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task AckAsync_for_an_undeclared_consumer_group_throws() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");

            var act = () => consumer.AckAsync("orders", "undeclared-group", 0, 0);

            (await act.Should().ThrowAsync<ArgumentException>()).Which.Message.Should().Contain("undeclared-group");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }
}
