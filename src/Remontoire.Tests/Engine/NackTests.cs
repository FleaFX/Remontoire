using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Messaging;
using Remontoire.Storage;

namespace Remontoire.Engine;

// The embedded engine's nack semantics: a nacked message comes back after its delay or the group's
// backoff, before any later one, with the next attempt; after the group's last retry it is parked
// and the group moves on. Pending retries and parked messages live in the ack index, rebuilt from
// the same Raft log every start, so both survive a restart.
public class NackTests {
    static string NewDataDirectory() => Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    // Stops the host too when the start fails, before rethrowing — an unstarted host that keeps its
    // WAL open makes the data directory's cleanup fail.
    static async Task<IHost> StartAsync(string dataDirectory) {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddRemontoire().AddEmbedded(options => options.DataDirectory = dataDirectory);

        var host = builder.Build();
        try {
            await host.StartAsync();
        } catch {
            await StopAsync(host);
            throw;
        }
        return host;
    }

    static async Task StopAsync(IHost host) {
        await host.StopAsync();
        host.Dispose();
    }

    // One stream per test: a fresh ConsumeAsync restarts at the group's watermark and never
    // exercises the go-back rule a retry needs.
    static Task<RemontoireMessage> NextAsync(ConsumeStream messages) => messages.NextAsync();

    [Fact]
    public async Task A_nacked_message_comes_back_after_its_delay_before_any_later_message() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing");
            await producer.PublishAsync("orders", "key-0", "zero"u8.ToArray());
            await producer.PublishAsync("orders", "key-1", "one"u8.ToArray());

            await using var messages = new ConsumeStream(consumer.ConsumeAsync("orders", "billing"));
            var first = await NextAsync(messages);
            first.Offset.Should().Be(0);
            first.Attempt.Should().Be(1);

            var stopwatch = Stopwatch.StartNew();
            await consumer.NackAsync("orders", "billing", first.ShardId, first.Offset, delay: TimeSpan.FromMilliseconds(300));
            var retried = await NextAsync(messages);
            stopwatch.Stop();

            retried.Offset.Should().Be(0);
            retried.Attempt.Should().Be(2);
            stopwatch.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(240),
                "the message comes back at the earliest after 80% of its delay, with margin");

            var second = await NextAsync(messages);
            second.Offset.Should().Be(1, "the nacked message comes back before any later one");
            second.Attempt.Should().Be(1);
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Without_a_delay_the_backoff_of_the_group_applies() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing", new ConsumerGroupOptions {
                InitialRetryDelay = TimeSpan.FromMilliseconds(200), MaxRetryDelay = TimeSpan.FromSeconds(1),
            });
            await producer.PublishAsync("orders", "key-0", "zero"u8.ToArray());

            await using var messages = new ConsumeStream(consumer.ConsumeAsync("orders", "billing"));
            var first = await NextAsync(messages);
            first.Attempt.Should().Be(1);

            var stopwatch = Stopwatch.StartNew();
            await consumer.NackAsync("orders", "billing", first.ShardId, first.Offset);
            var second = await NextAsync(messages);
            stopwatch.Stop();

            second.Offset.Should().Be(0);
            second.Attempt.Should().Be(2);
            stopwatch.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(160),
                "the first retry waits the group's initial delay — 80% of 200 ms, with margin");

            stopwatch.Restart();
            await consumer.NackAsync("orders", "billing", second.ShardId, second.Offset);
            var third = await NextAsync(messages);
            stopwatch.Stop();

            third.Offset.Should().Be(0);
            third.Attempt.Should().Be(3);
            stopwatch.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(320),
                "the second retry waits twice the initial delay — 80% of 400 ms, with margin");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task After_the_last_retry_the_message_is_parked_and_the_group_continues() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            var messagingRegistry = host.Services.GetRequiredService<MessagingGroupRegistry>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing", new ConsumerGroupOptions {
                MaxRetries = 1, InitialRetryDelay = TimeSpan.FromMilliseconds(10),
            });
            await producer.PublishAsync("orders", "key-0", "zero"u8.ToArray());
            await producer.PublishAsync("orders", "key-1", "one"u8.ToArray());

            await using var messages = new ConsumeStream(consumer.ConsumeAsync("orders", "billing"));
            var first = await NextAsync(messages);
            first.Attempt.Should().Be(1);
            await consumer.NackAsync("orders", "billing", first.ShardId, first.Offset);

            var second = await NextAsync(messages);
            second.Offset.Should().Be(0);
            second.Attempt.Should().Be(2);
            await consumer.NackAsync("orders", "billing", second.ShardId, second.Offset, reason: "boom");

            var next = await NextAsync(messages);
            next.Offset.Should().Be(1, "the parked message counts as handled — the group continues with the next one");
            next.Attempt.Should().Be(1);

            messagingRegistry.TryGet("orders", out var messaging).Should().BeTrue();
            var state = messaging.AckIndex.GetOrCreate("billing");
            state.IsParked(0).Should().BeTrue();
            state.Parked[0].Attempts.Should().Be(2);
            state.Parked[0].Reason.Should().Be("boom");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_pending_retry_and_a_parked_message_survive_a_restart() {
        var dataDirectory = NewDataDirectory();
        IHost? host1 = null;
        IHost? host2 = null;
        IHost? host3 = null;
        try {
            host1 = await StartAsync(dataDirectory);
            var admin = host1.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host1.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host1.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing", new ConsumerGroupOptions { MaxRetries = 1 });
            await producer.PublishAsync("orders", "key-0", "zero"u8.ToArray());
            await producer.PublishAsync("orders", "key-1", "one"u8.ToArray());

            await using (var messages = new ConsumeStream(consumer.ConsumeAsync("orders", "billing"))) {
                var first = await NextAsync(messages);
                first.Offset.Should().Be(0);
                await consumer.NackAsync("orders", "billing", first.ShardId, first.Offset, delay: TimeSpan.FromSeconds(1));
            }
            await StopAsync(host1);
            host1 = null;

            host2 = await StartAsync(dataDirectory);
            var secondConsumer = host2.Services.GetRequiredService<IRemontoireConsumer>();
            await using (var messages = new ConsumeStream(secondConsumer.ConsumeAsync("orders", "billing"))) {
                var first = await NextAsync(messages);
                first.Offset.Should().Be(0);
                first.Attempt.Should().Be(2, "the pending retry survived the restart");

                var second = await NextAsync(messages);
                second.Offset.Should().Be(1);
                second.Attempt.Should().Be(1, "a fresh stream that reaches the retried message delivers it once, not twice");

                // The second nack uses up the group's last retry: the message parks.
                await secondConsumer.NackAsync("orders", "billing", first.ShardId, first.Offset);
            }
            await StopAsync(host2);
            host2 = null;

            host3 = await StartAsync(dataDirectory);
            var thirdConsumer = host3.Services.GetRequiredService<IRemontoireConsumer>();
            await using (var messages = new ConsumeStream(thirdConsumer.ConsumeAsync("orders", "billing"))) {
                var first = await NextAsync(messages);
                first.Offset.Should().Be(1, "the parked message counts as handled across the restart");
                first.Attempt.Should().Be(1);
            }
        } finally {
            if (host1 is not null)
                await StopAsync(host1);
            if (host2 is not null)
                await StopAsync(host2);
            if (host3 is not null)
                await StopAsync(host3);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task NackAsync_rejects_a_negative_delay() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing");

            var act = () => consumer.NackAsync("orders", "billing", 0, 0, delay: TimeSpan.FromSeconds(-1));

            await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task NackAsync_for_an_undeclared_consumer_group_throws() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");

            var act = () => consumer.NackAsync("orders", "undeclared-group", 0, 0);

            (await act.Should().ThrowAsync<ArgumentException>()).Which.Message.Should().Contain("undeclared-group");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_nack_after_an_ack_does_nothing() {
        var dataDirectory = NewDataDirectory();
        IHost? host = null;
        try {
            host = await StartAsync(dataDirectory);
            var admin = host.Services.GetRequiredService<IRemontoireAdmin>();
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            var messagingRegistry = host.Services.GetRequiredService<MessagingGroupRegistry>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing");
            await producer.PublishAsync("orders", "key-0", "zero"u8.ToArray());
            await producer.PublishAsync("orders", "key-1", "one"u8.ToArray());

            await using var messages = new ConsumeStream(consumer.ConsumeAsync("orders", "billing"));
            var first = await NextAsync(messages);
            await consumer.AckAsync("orders", "billing", first.ShardId, first.Offset);

            await consumer.NackAsync("orders", "billing", first.ShardId, first.Offset);

            var second = await NextAsync(messages);
            second.Offset.Should().Be(1, "the late nack changed nothing — the next message is the one after the acked one");
            second.Attempt.Should().Be(1);
            messagingRegistry.TryGet("orders", out var messaging).Should().BeTrue();
            messaging.AckIndex.GetOrCreate("billing").Retry.Should().BeNull("a nack for an already-acked offset schedules no retry");
        } finally {
            if (host is not null)
                await StopAsync(host);
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }
}
