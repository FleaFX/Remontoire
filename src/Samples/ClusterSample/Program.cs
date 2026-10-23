using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Remontoire;
using Remontoire.Cluster;
using Remontoire.Engine;
using Remontoire.Server;

// One node of a three-node cluster: every node hosts the data group "group-1" and a replica of the
// meta group, with the same members. Configured from the environment (see docker-compose.yml):
//   REMONTOIRE_NODE_ID             this node, for example "node-1"
//   REMONTOIRE_NODES               every node's host name, for example "node-1,node-2,node-3"
// Optional, for TLS with a client certificate on the peer port only (see docker-compose.tls.yml):
//   REMONTOIRE_PEER_CERTIFICATE    this node's certificate with its private key (.pfx, no password)
//   REMONTOIRE_PEER_CA             the CA that signed every node certificate (.crt)
// Streams and consumer groups are declared by the clients (AddClient with AddStream/AddConsumerGroup),
// through the admin endpoint on the client port.
const int ClientPort = 6000;
const int PeerPort = 6001;
// Plain HTTP/1.1 for probes: the client and peer ports speak HTTP/2 only.
const int HealthPort = 8080;

var nodeId = Environment.GetEnvironmentVariable("REMONTOIRE_NODE_ID")
    ?? throw new InvalidOperationException("REMONTOIRE_NODE_ID is not set.");
var nodes = (Environment.GetEnvironmentVariable("REMONTOIRE_NODES")
    ?? throw new InvalidOperationException("REMONTOIRE_NODES is not set.")).Split(',');

var certificatePath = Environment.GetEnvironmentVariable("REMONTOIRE_PEER_CERTIFICATE");
var caPath = Environment.GetEnvironmentVariable("REMONTOIRE_PEER_CA");
var peerCertificate = certificatePath is null ? null : X509CertificateLoader.LoadPkcs12FromFile(certificatePath, password: null);
var peerCa = caPath is null ? null : X509CertificateLoader.LoadCertificateFromFile(caPath);
var peerScheme = peerCertificate is null ? "http" : "https";

// Peers replicate over the peer port; a redirect sends clients to the client port.
var peers = nodes.Where(node => node != nodeId).Select(node => new RaftPeerOptions {
    NodeId = node,
    Address = $"{peerScheme}://{node}:{PeerPort}",
    ClientAddress = $"http://{node}:{ClientPort}",
}).ToList();

var builder = WebApplication.CreateBuilder(args);
builder.Logging.AddJsonConsole(options => options.IncludeScopes = true);

builder.WebHost.ConfigureKestrel(options => {
    options.Listen(IPAddress.Any, HealthPort, listen => listen.Protocols = HttpProtocols.Http1);
    options.Listen(IPAddress.Any, ClientPort, listen => listen.Protocols = HttpProtocols.Http2);
    options.Listen(IPAddress.Any, PeerPort, listen => {
        listen.Protocols = HttpProtocols.Http2;
        if (peerCertificate is not null)
            listen.UseHttps(https => {
                https.ServerCertificate = peerCertificate;
                https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                https.ClientCertificateValidation = (certificate, _, _) => IsSignedBy(peerCa!, certificate);
            });
    });
});

builder.Services.AddRemontoire().AddCluster(options => {
    options.Groups = [new RaftGroupOptions { GroupId = "group-1", NodeId = nodeId, DataDirectory = $"data/{nodeId}/group-1", Peers = peers }];
    options.MetaGroup = new MetaGroupOptions { NodeId = nodeId, DataDirectory = $"data/{nodeId}/meta", Peers = peers };
    options.ClientAddress = $"http://{nodeId}:{ClientPort}";
    if (peerCertificate is not null)
        options.ConfigureHttpHandler = handler => handler.SslOptions = new SslClientAuthenticationOptions {
            ClientCertificates = [peerCertificate],
            RemoteCertificateValidationCallback = (_, certificate, _, _) => certificate is X509Certificate2 candidate && IsSignedBy(peerCa!, candidate),
        };
}).AddServer();
builder.Services.AddHealthChecks()
    .AddRemontoireLiveness()
    .AddRemontoireReadiness()
    .AddRemontoireDiskSpace();

var app = builder.Build();
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = check => check.Tags.Contains("live") }).RequireHost($"*:{HealthPort}");
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") }).RequireHost($"*:{HealthPort}");
app.MapRemontoireCluster().RequireHost($"*:{PeerPort}");
app.MapRemontoire().RequireHost($"*:{ClientPort}");
app.MapRemontoireAdmin().RequireHost($"*:{ClientPort}");
app.Run();

// Trusts exactly the certificates the cluster's own CA signed, whatever their host name, and
// ignores the platform trust store.
static bool IsSignedBy(X509Certificate2 ca, X509Certificate2 certificate) {
    using var chain = new X509Chain();
    chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
    chain.ChainPolicy.CustomTrustStore.Add(ca);
    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
    return chain.Build(certificate);
}
