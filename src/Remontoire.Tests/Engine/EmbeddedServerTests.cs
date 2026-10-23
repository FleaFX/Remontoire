using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire;
using Remontoire.Client;
using Remontoire.Server;

namespace Remontoire.Engine;

// The same engine, reached over the wire instead of in-process: the gRPC endpoints of
// Remontoire.Server mapped on an embedded node, exercised by Remontoire.Client over a real
// loopback socket — the client's own admin included.
[Collection("RealNetwork")]
public class EmbeddedServerTests {
    static int FreePort() {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        } finally {
            listener.Stop();
        }
    }

    (WebApplication App, string Address) StartApp(int port, string dataDirectory) {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, port, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Services.AddRemontoire().AddEmbedded(options => {
            options.DataDirectory = dataDirectory;
            options.ClientAddress = new Uri($"http://127.0.0.1:{port}");
        }).AddServer();

        var app = builder.Build();
        app.MapRemontoire();
        app.MapRemontoireAdmin();
        return (app, $"http://127.0.0.1:{port}");
    }

    ServiceProvider BuildClient(string address) {
        var services = new ServiceCollection();
        services.AddRemontoire().AddClient(options => {
            options.MetaGroupSeedAddresses = [new Uri(address)];
            options.MaxRedirectAttempts = 20;
            options.RedirectRetryDelay = TimeSpan.FromMilliseconds(50);
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task A_remote_client_publishes_consumes_and_acks_through_the_server_endpoints() {
        var port = FreePort();
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        WebApplication? app = null;
        ServiceProvider? provider = null;
        try {
            app = StartApp(port, dataDirectory).App;
            await app.StartAsync();

            provider = BuildClient($"http://127.0.0.1:{port}");
            var producer = provider.GetRequiredService<IRemontoireProducer>();
            var consumer = provider.GetRequiredService<IRemontoireConsumer>();
            var admin = provider.GetRequiredService<IRemontoireAdmin>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing");

            // A freshly constructed connection's watcher needs a moment to catch up before the
            // stream is known to it — same race the client's own retry choices anticipate.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            PublishResult result;
            while (true) {
                try {
                    result = await producer.PublishAsync("orders", "key-1", "hello"u8.ToArray());
                    break;
                } catch (Exception ex) when (ex is ArgumentException or RemontoireUnavailableException && DateTime.UtcNow < deadline) {
                    await Task.Delay(50);
                }
            }

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10))) {
                await foreach (var message in consumer.ConsumeAsync("orders", "billing", cts.Token)) {
                    message.Offset.Should().Be(result.Offset);
                    message.Payload.ToArray().Should().Equal("hello"u8.ToArray());
                    await consumer.AckAsync("orders", "billing", message.ShardId, message.Offset);
                    break;
                }
            }
        } finally {
            if (provider is not null)
                await provider.DisposeAsync();
            if (app is not null) {
                await app.StopAsync();
                await app.DisposeAsync();
            }
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_remote_client_cannot_consume_an_undeclared_consumer_group() {
        var port = FreePort();
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        WebApplication? app = null;
        ServiceProvider? provider = null;
        try {
            app = StartApp(port, dataDirectory).App;
            await app.StartAsync();

            provider = BuildClient($"http://127.0.0.1:{port}");
            var admin = provider.GetRequiredService<IRemontoireAdmin>();
            var consumer = provider.GetRequiredService<IRemontoireConsumer>();
            await admin.EnsureStreamAsync("orders");

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await using var enumerator = consumer.ConsumeAsync("orders", "undeclared-group", cts.Token).GetAsyncEnumerator(cts.Token);

            var act = () => enumerator.MoveNextAsync().AsTask();

            (await act.Should().ThrowAsync<ArgumentException>()).Which.Message.Should().Contain("undeclared-group");
        } finally {
            if (provider is not null)
                await provider.DisposeAsync();
            if (app is not null) {
                await app.StopAsync();
                await app.DisposeAsync();
            }
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_remote_client_gets_a_nacked_message_again_before_any_later_one() {
        var port = FreePort();
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        WebApplication? app = null;
        ServiceProvider? provider = null;
        try {
            app = StartApp(port, dataDirectory).App;
            await app.StartAsync();

            provider = BuildClient($"http://127.0.0.1:{port}");
            var producer = provider.GetRequiredService<IRemontoireProducer>();
            var consumer = provider.GetRequiredService<IRemontoireConsumer>();
            var admin = provider.GetRequiredService<IRemontoireAdmin>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing");

            // All three published before consuming, so the server already has the later messages in
            // flight by the time m0 is nacked — what the client must drop until it went back.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (true) {
                try {
                    await producer.PublishAsync("orders", "key-0", "zero"u8.ToArray());
                    break;
                } catch (Exception ex) when (ex is ArgumentException or RemontoireUnavailableException && DateTime.UtcNow < deadline) {
                    await Task.Delay(50);
                }
            }
            await producer.PublishAsync("orders", "key-1", "one"u8.ToArray());
            await producer.PublishAsync("orders", "key-2", "two"u8.ToArray());

            await using var messages = new ConsumeStream(consumer.ConsumeAsync("orders", "billing"));
            var first = await NextAsync(messages);
            first.Offset.Should().Be(0);
            first.Attempt.Should().Be(1);

            await consumer.NackAsync("orders", "billing", first.ShardId, first.Offset, delay: TimeSpan.FromMilliseconds(200));

            var retried = await NextAsync(messages);
            retried.Offset.Should().Be(0);
            retried.Attempt.Should().Be(2, "the nacked message comes back first, once the server went back to it");

            var second = await NextAsync(messages);
            second.Offset.Should().Be(1);
            second.Attempt.Should().Be(1);
            var third = await NextAsync(messages);
            third.Offset.Should().Be(2);
            third.Attempt.Should().Be(1);
        } finally {
            if (provider is not null)
                await provider.DisposeAsync();
            if (app is not null) {
                await app.StopAsync();
                await app.DisposeAsync();
            }
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task A_remote_client_parks_a_message_after_the_last_retry() {
        var port = FreePort();
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        WebApplication? app = null;
        ServiceProvider? provider = null;
        try {
            app = StartApp(port, dataDirectory).App;
            await app.StartAsync();

            provider = BuildClient($"http://127.0.0.1:{port}");
            var producer = provider.GetRequiredService<IRemontoireProducer>();
            var consumer = provider.GetRequiredService<IRemontoireConsumer>();
            var admin = provider.GetRequiredService<IRemontoireAdmin>();
            await admin.EnsureStreamAsync("orders");
            await admin.EnsureConsumerGroupAsync("orders", "billing", new ConsumerGroupOptions { MaxRetries = 0 });

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (true) {
                try {
                    await producer.PublishAsync("orders", "key-0", "zero"u8.ToArray());
                    break;
                } catch (Exception ex) when (ex is ArgumentException or RemontoireUnavailableException && DateTime.UtcNow < deadline) {
                    await Task.Delay(50);
                }
            }
            await producer.PublishAsync("orders", "key-1", "one"u8.ToArray());

            await using var messages = new ConsumeStream(consumer.ConsumeAsync("orders", "billing"));
            var first = await NextAsync(messages);
            first.Offset.Should().Be(0);
            first.Attempt.Should().Be(1);

            await consumer.NackAsync("orders", "billing", first.ShardId, first.Offset, reason: "boom");

            var second = await NextAsync(messages);
            second.Offset.Should().Be(1, "a group with no retries parks at the first nack — the next message follows");
            second.Attempt.Should().Be(1);
        } finally {
            if (provider is not null)
                await provider.DisposeAsync();
            if (app is not null) {
                await app.StopAsync();
                await app.DisposeAsync();
            }
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    static Task<RemontoireMessage> NextAsync(ConsumeStream messages) => messages.NextAsync();
}
