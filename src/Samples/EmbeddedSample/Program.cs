using System.Collections.Concurrent;
using System.Text;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Remontoire;
using Remontoire.Engine;

// Remontoire embedded in a web app: POST /greetings?name=… publishes a greeting, a background
// consumer handles and acks each one, and GET /greetings lists the greetings handled since start.
// The data lives in the setting Remontoire:DataDirectory ("data" by default).
// The stream and the consumer group are declared on the builder and exist before the consumer starts.
// The host owns observability: health at /health/live and /health/ready, Prometheus metrics at
// /metrics, and traces over OTLP once OTEL_EXPORTER_OTLP_ENDPOINT is set. Requests are traced as
// well: a publish span is the child of its request, and an untraced request would drop it.
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddRemontoire()
    .AddEmbedded(options => options.DataDirectory = builder.Configuration["Remontoire:DataDirectory"] ?? "data")
    .AddStream(GreetingConsumer.StreamName)
    .AddConsumerGroup(GreetingConsumer.StreamName, GreetingConsumer.ConsumerGroup);
builder.Services.AddSingleton<HandledGreetings>();
builder.Services.AddHostedService<GreetingConsumer>();

builder.Services.AddHealthChecks()
    .AddRemontoireLiveness()
    .AddRemontoireReadiness()
    .AddRemontoireDiskSpace();
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("Remontoire").AddPrometheusExporter())
    .WithTracing(tracing => {
        tracing.AddSource("Remontoire").AddAspNetCoreInstrumentation();
        if (builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"] is not null)
            tracing.AddOtlpExporter();
    });

var app = builder.Build();
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = check => check.Tags.Contains("live") });
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });
app.MapPrometheusScrapingEndpoint();
app.MapPost("/greetings", async (string name, IRemontoireProducer producer) => {
    var result = await producer.PublishAsync(GreetingConsumer.StreamName, partitionKey: name, Encoding.UTF8.GetBytes(name));
    return Results.Accepted(value: new { result.Offset });
});
app.MapGet("/greetings", (HandledGreetings handled) => handled.Names);
app.Run();

/// <summary>
/// The entry point, public so a <c>WebApplicationFactory</c> can host this app in a test.
/// </summary>
public partial class Program { }

/// <summary>
/// The greetings the consumer has handled since this process started, in order.
/// </summary>
sealed class HandledGreetings {
    readonly ConcurrentQueue<string> _names = new();

    /// <summary>
    /// The handled greetings.
    /// </summary>
    public IReadOnlyCollection<string> Names => _names.ToArray();

    /// <summary>
    /// Records one handled greeting.
    /// </summary>
    /// <param name="name">The greeted name.</param>
    public void Add(string name) => _names.Enqueue(name);
}

/// <summary>
/// Handles and acks every greeting; the stream and the consumer group are declared on the builder.
/// </summary>
sealed class GreetingConsumer(IRemontoireConsumer consumer, HandledGreetings handled) : BackgroundService {
    /// <summary>
    /// The stream greetings are published to.
    /// </summary>
    public const string StreamName = "greetings";

    /// <summary>
    /// The consumer group the greetings are handled with.
    /// </summary>
    public const string ConsumerGroup = "greeter";

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        await foreach (var message in consumer.ConsumeAsync(StreamName, ConsumerGroup, stoppingToken)) {
            handled.Add(Encoding.UTF8.GetString(message.Payload.Span));
            await consumer.AckAsync(StreamName, ConsumerGroup, message.ShardId, message.Offset, stoppingToken);
        }
    }
}
