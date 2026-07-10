using Microsoft.AspNetCore.Server.Kestrel.Core;
using Remontoire.Raft.Grpc;
using Remontoire.Server;
using Remontoire.Server.Grpc;
using Remontoire.Sharding;

var builder = WebApplication.CreateBuilder(args);

// No mTLS yet (a later phase) — peers talk plain HTTP/2, which Kestrel only serves when told to.
builder.WebHost.ConfigureKestrel(options =>
    options.ConfigureEndpointDefaults(listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

builder.Services.Configure<RaftServerOptions>(builder.Configuration.GetSection("Raft"));
builder.Services.AddSingleton<RaftReplicaRegistry>();
builder.Services.AddSingleton<MessagingGroupRegistry>();
builder.Services.AddSingleton<LeaderAddressDirectory>();
builder.Services.AddSingleton<ShardAssignmentTable>();
builder.Services.AddSingleton<MetaLogJournal>();
builder.Services.AddHostedService<RaftReplicaHostedService>();
builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<RaftTransportGrpcService>();
app.MapGrpcService<RemontoireClientGrpcService>();

// Only a process that also hosts a meta-group replica has anything to serve here — a node
// without one has no MetaLogJournal content and nothing else would ever route to it anyway.
if (builder.Configuration.GetSection("Raft:MetaGroup").Exists())
    app.MapGrpcService<ShardAssignmentMetaGrpcService>();

app.MapGet("/", () => "Hello World!");

app.Run();
