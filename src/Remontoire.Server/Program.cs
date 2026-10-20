using System.Net;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Trace;
using Remontoire.Raft.Grpc;
using Remontoire.Server;
using Remontoire.Server.Grpc;
using Remontoire.Server.HealthChecks;
using Remontoire.Sharding;

var builder = WebApplication.CreateBuilder(args);

// One JSON object per line, with scopes included — IncludeScopes surfaces the NodeId/ShardGroupId
// scope RaftReplicaHostedService opens per group onto every log line written within it.
builder.Logging.AddJsonConsole(options => options.IncludeScopes = true);

var raftOptions = builder.Configuration.GetSection("Raft").Get<RaftServerOptions>() ?? new RaftServerOptions();

// Two endpoints, one process: peer traffic and client traffic on separate ports, so a deployment
// can expose them on different networks. Plain HTTP/2 — TLS, client certificates and
// authentication are configured by the host, not by Remontoire.
builder.WebHost.ConfigureKestrel(options => {
    options.Listen(IPAddress.Any, raftOptions.PeerPort, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
    options.Listen(IPAddress.Any, raftOptions.ClientPort, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
});

builder.Services.Configure<RaftServerOptions>(builder.Configuration.GetSection("Raft"));
builder.Services.AddSingleton<RaftReplicaRegistry>();
builder.Services.AddSingleton<MessagingGroupRegistry>();
builder.Services.AddSingleton<LeaderAddressDirectory>();
builder.Services.AddSingleton<ShardAssignmentTable>();
builder.Services.AddSingleton<MetaLogJournal>();
builder.Services.AddSingleton<MigrationAdmissionGate>();
builder.Services.AddSingleton<ReshardOrchestrator>();
builder.Services.AddHostedService<RaftReplicaHostedService>();

builder.Services.AddGrpc();

builder.Services.AddHealthChecks()
    .AddCheck<RaftLivenessCheck>("raft-liveness", tags: ["live"])
    .AddCheck<RaftReadinessCheck>("raft-readiness", tags: ["ready"])
    .AddCheck<DiskSpaceReadinessCheck>("disk-space", tags: ["ready"]);

// AddAspNetCoreInstrumentation covers Publish/Ack/Consume and the Raft transport's own server side
// for free (both are plain Grpc.AspNetCore services). AddGrpcClientInstrumentation covers
// Remontoire.Client and RaftGrpcTransport's peer-to-peer replication calls for free (both ride
// Grpc.Net.Client), including automatic W3C trace-context propagation over gRPC metadata — no
// manual code needed for either. AddSource("Remontoire.Raft") picks up the wal-append/
// raft-replicate spans RaftActivitySource starts manually, since those live inside one method call,
// not at an RPC boundary auto-instrumentation could ever see. Exporter: console, a zero-infra
// starting point — swapping in OTLP-to-a-collector later needs no code change beyond this one line.
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddGrpcClientInstrumentation()
        .AddSource("Remontoire.Raft")
        .AddConsoleExporter());

var app = builder.Build();

ObservableMetricsRegistration.Register(
    app.Services.GetRequiredService<RaftReplicaRegistry>(),
    app.Services.GetRequiredService<MessagingGroupRegistry>(),
    app.Services.GetRequiredService<ShardAssignmentTable>());

app.MapGrpcService<RaftTransportGrpcService>().RequireHost($"*:{raftOptions.PeerPort}");
app.MapGrpcService<RemontoireClientGrpcService>().RequireHost($"*:{raftOptions.ClientPort}");

// Only a process that also hosts a meta-group replica has anything to serve here — a node
// without one has no MetaLogJournal content and nothing else would ever route to it anyway. Lives
// on the client port: read-only table-snapshot and admin traffic, not a Raft-consensus RPC.
if (builder.Configuration.GetSection("Raft:MetaGroup").Exists()) {
    app.MapGrpcService<ShardAssignmentMetaGrpcService>().RequireHost($"*:{raftOptions.ClientPort}");
    app.MapGrpcService<RemontoireAdminGrpcService>().RequireHost($"*:{raftOptions.ClientPort}");
}

app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = check => check.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });

app.MapGet("/", () => "Hello World!");

app.Run();
