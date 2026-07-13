using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire.Raft.V1;
using Remontoire.Security;

namespace Remontoire.Raft.Grpc;

// Laag-4, the literal node-mTLS half of the fase-8 exit criterion: "node-naar-node-verkeer zonder
// geldig cluster-certificaat wordt geweigerd." A real Kestrel host with
// ClientCertificateMode.RequireCertificate, validated by the actual production
// PeerCertificateValidator (not a reimplementation) — not the PeerCertificateRequiredInterceptor,
// which is defense-in-depth only (§5.1); this test proves the TLS handshake itself is the real gate.
public class RaftGrpcMtlsClusterTests {
    static async Task<WebApplication> StartServerAsync(X509Certificate2 caPublic, X509Certificate2 serverCertificate) {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<RaftReplicaRegistry>();
        builder.Services.AddGrpc();

        var validator = new PeerCertificateValidator(caPublic, expectedSubject: null);
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(IPAddress.Loopback, 0, listenOptions => {
                listenOptions.Protocols = HttpProtocols.Http2;
                listenOptions.UseHttps(https => {
                    https.ServerCertificate = serverCertificate;
                    https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                    https.ClientCertificateValidation = (certificate, chain, errors) => validator.Validate(certificate, chain!, errors);
                });
            }));

        var app = builder.Build();
        app.MapGrpcService<RaftTransportGrpcService>();
        await app.StartAsync();
        return app;
    }

    static GrpcChannel CreateClientChannel(Uri address, X509Certificate2 caPublic, X509Certificate2? clientCertificate) {
        var validator = new PeerCertificateValidator(caPublic, expectedSubject: null);
        var handler = new SocketsHttpHandler {
            SslOptions = new SslClientAuthenticationOptions {
                ClientCertificates = clientCertificate is null ? [] : [clientCertificate],
                RemoteCertificateValidationCallback = (_, certificate, chain, errors) =>
                    certificate is X509Certificate2 candidate && chain is not null && validator.Validate(candidate, chain, errors),
            },
        };
        return GrpcChannel.ForAddress(address, new GrpcChannelOptions { HttpHandler = handler, DisposeHttpClient = true });
    }

    [Fact]
    public async Task A_CA_signed_client_certificate_completes_an_RPC() {
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var nodeCertificate = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1");

        await using var server = await StartServerAsync(caPublic, nodeCertificate);
        using var channel = CreateClientChannel(new Uri(server.Urls.First()), caPublic, nodeCertificate);
        var client = new RaftTransport.RaftTransportClient(channel);

        var act = () => client.RequestVoteAsync(new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-2" }).ResponseAsync;

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
        using var channel = CreateClientChannel(new Uri(server.Urls.First()), caPublic, rogueCertificate);
        var client = new RaftTransport.RaftTransportClient(channel);

        var act = () => client.RequestVoteAsync(new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-2" }).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.Unavailable,
            "the connection must never reach the application layer at all — this is a TLS-handshake-level rejection, not an RPC-level one");
    }

    [Fact]
    public async Task No_client_certificate_at_all_is_rejected_at_the_TLS_handshake() {
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var nodeCertificate = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1");

        await using var server = await StartServerAsync(caPublic, nodeCertificate);
        using var channel = CreateClientChannel(new Uri(server.Urls.First()), caPublic, clientCertificate: null);
        var client = new RaftTransport.RaftTransportClient(channel);

        var act = () => client.RequestVoteAsync(new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-2" }).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.Unavailable,
            "node-to-node traffic without a valid cluster certificate must be rejected — the literal fase-8 exit criterion");
    }
}
