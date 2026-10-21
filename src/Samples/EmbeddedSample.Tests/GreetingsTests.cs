using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using OpenTelemetry.Trace;

namespace EmbeddedSample.Tests;

public class GreetingsTests {
    [Fact]
    public async Task A_posted_greeting_is_handled_by_the_embedded_consumer() {
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            await using (var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
                    builder.UseSetting("Remontoire:DataDirectory", dataDirectory))) {
                using var client = factory.CreateClient();

                (await client.PostAsync("/greetings?name=Ada", content: null)).StatusCode.Should().Be(HttpStatusCode.Accepted);

                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
                string[]? greetings = null;
                while (DateTime.UtcNow < deadline) {
                    greetings = await client.GetFromJsonAsync<string[]>("/greetings");
                    if (greetings is ["Ada"])
                        break;

                    await Task.Delay(50);
                }

                greetings.Should().Equal("Ada");
                Directory.Exists(Path.Combine(dataDirectory, "groups", "greetings")).Should().BeTrue();
            }
        } finally {
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Health_metrics_and_traces_are_exported_through_the_host_s_own_setup() {
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var exported = new List<Activity>();
        try {
            await using (var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder => {
                    builder.UseSetting("Remontoire:DataDirectory", dataDirectory);
                    builder.ConfigureServices(services => services.ConfigureOpenTelemetryTracerProvider(tracing => tracing.AddInMemoryExporter(exported)));
                })) {
                using var client = factory.CreateClient();

                (await client.GetAsync("/health/ready")).StatusCode.Should().Be(HttpStatusCode.OK);

                (await client.PostAsync("/greetings?name=Grace", content: null)).StatusCode.Should().Be(HttpStatusCode.Accepted);

                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
                string[]? greetings = null;
                while (DateTime.UtcNow < deadline) {
                    greetings = await client.GetFromJsonAsync<string[]>("/greetings");
                    if (greetings is ["Grace"])
                        break;

                    await Task.Delay(50);
                }
                greetings.Should().Equal("Grace");

                string? metrics = null;
                while (DateTime.UtcNow < deadline) {
                    metrics = await (await client.GetAsync("/metrics")).Content.ReadAsStringAsync();
                    if (metrics.Contains("remontoire_messages_published_total") && metrics.Contains("remontoire_messages_acked_total"))
                        break;

                    await Task.Delay(50);
                }
                metrics.Should().Contain("remontoire_messages_published_total");
                metrics.Should().Contain("remontoire_messages_acked_total");

                // The in-memory exporter exports a span when it stops, so both appear only after the
                // consumer handed the greeting over.
                Activity? publish = null;
                Activity? receive = null;
                deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
                while (DateTime.UtcNow < deadline) {
                    publish = exported.FirstOrDefault(activity => activity.DisplayName == "publish greetings" && activity.Kind == ActivityKind.Producer);
                    receive = exported.FirstOrDefault(activity => activity.DisplayName == "receive greetings" && activity.Kind == ActivityKind.Consumer);
                    if (publish is not null && receive is not null)
                        break;

                    await Task.Delay(50);
                }

                publish.Should().NotBeNull("the host's own tracing setup must export the publish span");
                receive.Should().NotBeNull("the host's own tracing setup must export the receive span");
                receive!.Links.Should().Contain(link => link.Context.TraceId == publish!.TraceId);
            }
        } finally {
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }
}
