using System.Net;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using OpenTelemetry.Trace;
using Remontoire.Raft.Grpc;
using Remontoire.Security;
using Remontoire.Server;
using Remontoire.Server.Grpc;
using Remontoire.Server.HealthChecks;
using Remontoire.Sharding;

var builder = WebApplication.CreateBuilder(args);

// One JSON object per line, with scopes included — IncludeScopes surfaces the NodeId/ShardGroupId
// scope RaftReplicaHostedService opens per group onto every log line written within it.
builder.Logging.AddJsonConsole(options => options.IncludeScopes = true);

var raftOptions = builder.Configuration.GetSection("Raft").Get<RaftServerOptions>() ?? new RaftServerOptions();
var secure = !raftOptions.Mtls.AllowInsecureTransport;
var mtlsCredentials = secure ? ClusterMtlsCredentialsLoader.Load(raftOptions.Mtls) : null;

// Server-side never knows, before validation, which specific peer is connecting — so unlike
// RaftGrpcTransport's own outbound-only validators (one expected subject per dialed peer), this is
// the union of every configured peer's expected subject across every group this node hosts. Empty
// (nobody ever set ExpectedCertificateSubject) falls back to null — CA-signature-only, same as an
// individual unconfigured peer already means (§5.4). Mirrors RaftReplicaHostedService's own
// allPeers/expectedCertificateSubjects computation, since Kestrel's endpoint config here runs
// before that hosted service ever starts.
var expectedCertificateSubjects = raftOptions.Groups.SelectMany(group => group.Peers)
    .Concat(raftOptions.MetaGroup?.Peers ?? [])
    .Select(peer => peer.ExpectedCertificateSubject)
    .Where(subject => subject is not null)
    .Select(subject => subject!)
    .Distinct()
    .ToArray();
var peerCertificateValidator = mtlsCredentials is null ? null
    : new PeerCertificateValidator(mtlsCredentials.CaCertificate, expectedCertificateSubjects.Length == 0 ? null : expectedCertificateSubjects);

// Two endpoints, one process: the peer port requires a client certificate (node-to-node, §5.3),
// the client port never does (client-to-cluster auth is JWT, §2). AllowInsecureTransport skips TLS
// on both — dev/test only, never a silent default (§6.1).
builder.WebHost.ConfigureKestrel(options => {
    options.Listen(IPAddress.Any, raftOptions.PeerPort, listenOptions => {
        listenOptions.Protocols = HttpProtocols.Http2;
        if (secure)
            listenOptions.UseHttps(https => {
                https.ServerCertificateSelector = (_, _) => mtlsCredentials!.NodeCertificate;
                https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                https.ClientCertificateValidation = (certificate, chain, errors) => peerCertificateValidator!.Validate(certificate, chain!, errors);
            });
    });

    options.Listen(IPAddress.Any, raftOptions.ClientPort, listenOptions => {
        listenOptions.Protocols = HttpProtocols.Http2;
        if (secure)
            listenOptions.UseHttps(https => {
                https.ServerCertificateSelector = (_, _) => mtlsCredentials!.NodeCertificate;
                https.ClientCertificateMode = ClientCertificateMode.NoCertificate;
            });
    });
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

// Client authentication + authorization — RaftTransportGrpcService (node-to-node) never carries
// a bearer token, so the interceptor below is registered per-service, not on AddGrpc() globally.
builder.Services.Configure<RemontoireSecurityOptions>(builder.Configuration.GetSection("Security"));
builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options => {
        var security = builder.Configuration.GetSection("Security").Get<RemontoireSecurityOptions>() ?? new RemontoireSecurityOptions();
        options.Authority = security.Authority;
        options.Audience = security.Audience;
        options.RequireHttpsMetadata = security.RequireHttpsMetadata;
        options.MapInboundClaims = false;
        options.TokenValidationParameters.RoleClaimType = security.RoleClaimType;
        options.TokenValidationParameters.NameClaimType = security.SubjectClaimType;
    });
builder.Services.AddAuthorization();
builder.Services.AddSingleton<RemontoireAuthorizer>();

// PeerCertificateRequiredInterceptor is defense-in-depth only (§5.1) — the real trust decision for
// node-to-node traffic already happened at the TLS handshake above (ClientCertificateMode.RequireCertificate).
builder.Services.AddGrpc()
    .AddServiceOptions<RemontoireClientGrpcService>(options => {
        options.Interceptors.Add<RemontoireAuthenticationInterceptor>();
        options.Interceptors.Add<RemontoireAuthorizationInterceptor>();
    })
    .AddServiceOptions<RaftTransportGrpcService>(options => options.Interceptors.Add<PeerCertificateRequiredInterceptor>());

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

app.UseAuthentication();
app.UseAuthorization();

app.MapGrpcService<RaftTransportGrpcService>().RequireHost($"*:{raftOptions.PeerPort}");
app.MapGrpcService<RemontoireClientGrpcService>().RequireHost($"*:{raftOptions.ClientPort}");

// Only a process that also hosts a meta-group replica has anything to serve here — a node
// without one has no MetaLogJournal content and nothing else would ever route to it anyway. Lives
// on the client port: read-only table-snapshot traffic, not a Raft-consensus RPC, so it only needs
// ordinary TLS, not peer mTLS (§5.3).
if (builder.Configuration.GetSection("Raft:MetaGroup").Exists())
    app.MapGrpcService<ShardAssignmentMetaGrpcService>().RequireHost($"*:{raftOptions.ClientPort}");

app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = check => check.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });

app.MapGet("/", () => "Hello World!");

app.Run();
