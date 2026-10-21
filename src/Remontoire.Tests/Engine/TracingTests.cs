using System.Collections.Concurrent;
using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Remontoire;

namespace Remontoire.Engine;

// The wire protocol of a message's trace context: publish starts a Producer span and stores its
// context as the message's traceparent header, and every delivered message starts its own Consumer
// span, without a parent, linked back to the publish that header names. Each test drives a real
// embedded host over a uniquely named stream, collecting the stopped spans of the "Remontoire"
// source.
[Collection("Tracing")]
public class TracingTests {
    [Fact]
    public async Task Publish_starts_a_producer_span_and_stores_its_context_as_traceparent() {
        var stream = $"trace-{Guid.NewGuid():N}";
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        IHost? host = null;
        try {
            var stopped = new ConcurrentQueue<Activity>();
            using var listener = Listen("Remontoire", stopped);

            host = await StartHostAsync(dataDirectory);
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await host.Services.GetRequiredService<IRemontoireAdmin>().CreateStreamAsync(stream);

            await producer.PublishAsync(stream, "key-1", "one"u8.ToArray());
            var message = await FirstAsync(consumer, stream, "trace-group");

            var publish = stopped.Should().ContainSingle(a =>
                a.Kind == ActivityKind.Producer && a.GetTagItem("messaging.destination.name") as string == stream).Which;
            publish.DisplayName.Should().Be($"publish {stream}");
            publish.GetTagItem("messaging.system").Should().Be("remontoire");
            publish.GetTagItem("messaging.operation.type").Should().Be("send");
            publish.GetTagItem("messaging.operation.name").Should().Be("publish");

            message.Headers["traceparent"].Should().Be(publish.Id, "the publish span is current when the header is stored");
            message.Headers.Should().NotContainKey("correlation-id");

            var walAppend = stopped.Should().ContainSingle(a =>
                a.DisplayName == "wal-append" && a.TraceId == publish.TraceId).Which;
            walAppend.ParentSpanId.Should().Be(publish.SpanId, "the propose runs inside the publish span");
        } finally {
            if (host is not null) {
                await host.StopAsync();
                host.Dispose();
            }
            Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_traceparent_the_caller_supplied_is_kept() {
        const string callerTraceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        var stream = $"trace-{Guid.NewGuid():N}";
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        IHost? host = null;
        try {
            host = await StartHostAsync(dataDirectory);
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await host.Services.GetRequiredService<IRemontoireAdmin>().CreateStreamAsync(stream);

            await producer.PublishAsync(stream, "key-1", "one"u8.ToArray(),
                headers: new Dictionary<string, string> { ["traceparent"] = callerTraceparent });

            var message = await FirstAsync(consumer, stream, "trace-group");
            message.Headers["traceparent"].Should().Be(callerTraceparent, "a caller-supplied traceparent is never overwritten");
        } finally {
            if (host is not null) {
                await host.StopAsync();
                host.Dispose();
            }
            Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Each_received_message_gets_a_parentless_consumer_span_linked_to_its_publish() {
        var stream = $"trace-{Guid.NewGuid():N}";
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        IHost? host = null;
        try {
            var stopped = new ConcurrentQueue<Activity>();
            using var listener = Listen("Remontoire", stopped);

            host = await StartHostAsync(dataDirectory);
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await host.Services.GetRequiredService<IRemontoireAdmin>().CreateStreamAsync(stream);

            await producer.PublishAsync(stream, "key-1", "one"u8.ToArray());
            await producer.PublishAsync(stream, "key-2", "two"u8.ToArray());

            var messages = new List<RemontoireMessage>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var message in consumer.ConsumeAsync(stream, "trace-group", cts.Token)) {
                messages.Add(message);
                if (messages.Count == 2)
                    break;
            }
            messages.Should().HaveCount(2);

            var publishes = stopped.Where(a => a.Kind == ActivityKind.Producer && a.GetTagItem("messaging.destination.name") as string == stream).ToList();
            var receives = stopped.Where(a => a.Kind == ActivityKind.Consumer && a.GetTagItem("messaging.destination.name") as string == stream).ToList();
            publishes.Should().HaveCount(2);
            receives.Should().HaveCount(2);

            foreach (var receive in receives) {
                receive.DisplayName.Should().Be($"receive {stream}");
                receive.ParentSpanId.Should().Be(default(ActivitySpanId), "a receive starts a trace of its own, never a child of the hours-long consume stream");
                publishes.Select(publish => publish.TraceId).Should().NotContain(receive.TraceId);
                receive.GetTagItem("messaging.consumer.group.name").Should().Be("trace-group");

                var link = receive.Links.Should().ContainSingle("one link, to the publish of this very message").Which;
                publishes.Should().Contain(publish => publish.TraceId == link.Context.TraceId && publish.SpanId == link.Context.SpanId);
            }

            receives[0].TraceId.Should().NotBe(receives[1].TraceId, "each receive starts its own trace");
            receives.Select(receive => receive.Links.Single().Context.SpanId).Should().OnlyHaveUniqueItems("each receive links to its own publish");
        } finally {
            if (host is not null) {
                await host.StopAsync();
                host.Dispose();
            }
            Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Without_a_listener_on_Remontoire_publish_stores_the_caller_s_context() {
        var stream = $"trace-{Guid.NewGuid():N}";
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        IHost? host = null;
        try {
            const string testSourceName = "Remontoire.Tests.Tracing";
            var stopped = new ConcurrentQueue<Activity>();
            using var listener = Listen(testSourceName, stopped);

            host = await StartHostAsync(dataDirectory);
            var producer = host.Services.GetRequiredService<IRemontoireProducer>();
            var consumer = host.Services.GetRequiredService<IRemontoireConsumer>();
            await host.Services.GetRequiredService<IRemontoireAdmin>().CreateStreamAsync(stream);

            var source = new ActivitySource(testSourceName);
            Activity? ambient = null;
            using (ambient = source.StartActivity("ambient"))
                await producer.PublishAsync(stream, "key-1", "one"u8.ToArray());

            var withAmbient = await FirstAsync(consumer, stream, "trace-group");
            withAmbient.Headers["traceparent"].Should().Be(ambient!.Id, "without a listener there is no publish span — the caller's context is the one stored");
            stopped.Should().OnlyContain(a => a.Source.Name == testSourceName, "nobody listens on Remontoire, so no Remontoire span is ever started");

            await consumer.AckAsync(stream, "trace-group", withAmbient.ShardId, withAmbient.Offset);
            await producer.PublishAsync(stream, "key-2", "two"u8.ToArray());
            var withoutAmbient = await FirstAsync(consumer, stream, "trace-group");
            withoutAmbient.Headers.Should().NotContainKey("traceparent", "no listener, no ambient activity — nothing to store");
        } finally {
            if (host is not null) {
                await host.StopAsync();
                host.Dispose();
            }
            Directory.Delete(dataDirectory, recursive: true);
        }
    }

    static async Task<IHost> StartHostAsync(string dataDirectory) {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddRemontoire().AddEmbedded(options => options.DataDirectory = dataDirectory);
        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    static ActivityListener Listen(string sourceName, ConcurrentQueue<Activity> stopped) {
        var listener = new ActivityListener {
            ShouldListenTo = source => source.Name == sourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity => stopped.Enqueue(activity),
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    static async Task<RemontoireMessage> FirstAsync(IRemontoireConsumer consumer, string streamName, string consumerGroup) {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var message in consumer.ConsumeAsync(streamName, consumerGroup, cts.Token))
            return message;

        throw new InvalidOperationException($"No message arrived on stream '{streamName}' for group '{consumerGroup}'.");
    }
}
