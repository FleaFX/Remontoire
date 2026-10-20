using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire.Raft.V1;
using Remontoire.Tests;

namespace Remontoire.Raft.Grpc;

// The host's node-to-node TLS configuration, end to end: each Kestrel host requires a client
// certificate and validates it against the host's own CA, while the transport hook presents a
// node certificate and trusts that same CA. The TLS handshake is the real gate — traffic without
// a valid cluster certificate never reaches anything application-level.
[Collection("RealNetwork")]
public class RaftGrpcMtlsClusterTests {
    static async Task<WebApplication> StartServerAsync(X509Certificate2 caPublic, X509Certificate2 serverCertificate) {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<RaftReplicaRegistry>();
        builder.Services.AddGrpc();

        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(IPAddress.Loopback, 0, listenOptions => {
                listenOptions.Protocols = HttpProtocols.Http2;
                listenOptions.UseHttps(https => {
                    https.ServerCertificate = serverCertificate;
                    https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                    https.ClientCertificateValidation = (certificate, chain, _) => DevClusterCertificateFactory.IsSignedBy(caPublic, certificate, chain!);
                });
            }));

        var app = builder.Build();
        app.MapGrpcService<RaftTransportGrpcService>();
        await app.StartAsync();
        return app;
    }

    static Action<SocketsHttpHandler> PresentAndTrust(X509Certificate2 caPublic, X509Certificate2? clientCertificate) =>
        handler => handler.SslOptions = new SslClientAuthenticationOptions {
            ClientCertificates = clientCertificate is null ? [] : [clientCertificate],
            RemoteCertificateValidationCallback = (_, certificate, chain, _) =>
                certificate is X509Certificate2 candidate && chain is not null && DevClusterCertificateFactory.IsSignedBy(caPublic, candidate, chain),
        };

    [Fact]
    public async Task A_CA_signed_client_certificate_completes_an_RPC() {
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var nodeCertificate = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1");

        await using var server = await StartServerAsync(caPublic, nodeCertificate);
        using var transport = new RaftGrpcTransport([new RaftGroupMember("node-1", new Uri(server.Urls.First()))], TimeSpan.FromSeconds(5), PresentAndTrust(caPublic, nodeCertificate));

        var act = async () => await transport.RequestVoteAsync("node-1", new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-2" });

        // The TLS handshake succeeded — the call reached RaftTransportGrpcService's own logic,
        // which has no group registered in this minimal host.
        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.NotFound);
    }

    [Fact]
    public async Task A_certificate_signed_by_a_different_CA_is_rejected_at_the_TLS_handshake() {
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var nodeCertificate = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1");
        var (_, rogueCaWithKey) = DevClusterCertificateFactory.CreateCa("CN=Rogue CA");
        var rogueCertificate = DevClusterCertificateFactory.CreateLeaf(rogueCaWithKey, "CN=node-2");

        await using var server = await StartServerAsync(caPublic, nodeCertificate);
        using var transport = new RaftGrpcTransport([new RaftGroupMember("node-1", new Uri(server.Urls.First()))], TimeSpan.FromSeconds(5), PresentAndTrust(caPublic, rogueCertificate));

        var act = async () => await transport.RequestVoteAsync("node-1", new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-2" });

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.Unavailable,
            "the connection must never reach the application layer at all — this is a TLS-handshake-level rejection, not an RPC-level one");
    }

    [Fact]
    public async Task No_client_certificate_at_all_is_rejected_at_the_TLS_handshake() {
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var nodeCertificate = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1");

        await using var server = await StartServerAsync(caPublic, nodeCertificate);
        using var transport = new RaftGrpcTransport([new RaftGroupMember("node-1", new Uri(server.Urls.First()))], TimeSpan.FromSeconds(5), PresentAndTrust(caPublic, clientCertificate: null));

        var act = async () => await transport.RequestVoteAsync("node-1", new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-2" });

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.Unavailable,
            "node-to-node traffic without a valid cluster certificate is rejected by the host's Kestrel configuration");
    }

    [Fact]
    public async Task Three_nodes_with_host_configured_mTLS_elect_a_leader_and_commit_a_proposal() {
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var certificates = new[] {
            DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1"),
            DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-2"),
            DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-3"),
        };

        var hosts = new WebApplication[3];
        for (var i = 0; i < hosts.Length; i++)
            hosts[i] = await StartServerAsync(caPublic, certificates[i]);

        await RaftGrpcClusterTests.ElectAndCommitAsync(hosts, i => PresentAndTrust(caPublic, certificates[i]));
    }
}
