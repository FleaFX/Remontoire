using Microsoft.AspNetCore.Server.Kestrel.Core;
using Remontoire.Raft.Grpc;
using Remontoire.Server;
using Remontoire.Server.Grpc;

var builder = WebApplication.CreateBuilder(args);

// No mTLS yet (a later phase) — peers talk plain HTTP/2, which Kestrel only serves when told to.
builder.WebHost.ConfigureKestrel(options =>
    options.ConfigureEndpointDefaults(listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

builder.Services.Configure<RaftServerOptions>(builder.Configuration.GetSection("Raft"));
builder.Services.AddSingleton<RaftReplicaRegistry>();
builder.Services.AddSingleton<MessagingGroupRegistry>();
builder.Services.AddSingleton<LeaderAddressDirectory>();
builder.Services.AddHostedService<RaftReplicaHostedService>();
builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<RaftTransportGrpcService>();
app.MapGrpcService<RemontoireClientGrpcService>();
app.MapGet("/", () => "Hello World!");

app.Run();
