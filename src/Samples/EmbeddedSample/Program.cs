using System.Collections.Concurrent;
using System.Text;
using Remontoire;
using Remontoire.Engine;

// Remontoire embedded in a web app: POST /greetings?name=… publishes a greeting, a background
// consumer handles and acks each one, and GET /greetings lists the greetings handled since start.
// The data lives in the setting Remontoire:DataDirectory ("data" by default).
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddRemontoire().AddEmbedded(options => options.DataDirectory = builder.Configuration["Remontoire:DataDirectory"] ?? "data");
builder.Services.AddSingleton<HandledGreetings>();
builder.Services.AddHostedService<GreetingConsumer>();

var app = builder.Build();
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
/// Creates the greetings stream at startup, then handles and acks every greeting.
/// </summary>
sealed class GreetingConsumer(IRemontoireAdmin admin, IRemontoireConsumer consumer, HandledGreetings handled) : BackgroundService {
    /// <summary>
    /// The stream greetings are published to.
    /// </summary>
    public const string StreamName = "greetings";

    const string ConsumerGroup = "greeter";

    /// <summary>
    /// Creates the stream before the consumer starts, so it exists before the app takes requests.
    /// </summary>
    /// <param name="cancellationToken">Cancels startup.</param>
    public override async Task StartAsync(CancellationToken cancellationToken) {
        await admin.CreateStreamAsync(StreamName, cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        await foreach (var message in consumer.ConsumeAsync(StreamName, ConsumerGroup, stoppingToken)) {
            handled.Add(Encoding.UTF8.GetString(message.Payload.Span));
            await consumer.AckAsync(StreamName, ConsumerGroup, message.ShardId, message.Offset, stoppingToken);
        }
    }
}
