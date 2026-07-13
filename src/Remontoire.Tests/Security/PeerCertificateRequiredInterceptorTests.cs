using System.Net;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Raft.V1;
using Remontoire.Tests;

namespace Remontoire.Security;

// Laag-1-ish: exercises the interceptor's own conditional logic only, via a minimal middleware
// that sets HttpContext.Connection.ClientCertificate directly — not a real mTLS handshake (that's
// the node-mTLS end-to-end test, a later step). Whether Kestrel actually enforces
// ClientCertificateMode.RequireCertificate over a real TLS connection is that test's own concern;
// this one is only about what PeerCertificateRequiredInterceptor itself does once a certificate
// either is or isn't present on the connection.
public class PeerCertificateRequiredInterceptorTests {
    static async Task<WebApplication> StartHostAsync(X509Certificate2? clientCertificate) {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.ConfigureLoopbackHttp2());

        builder.Services.AddSingleton<RaftReplicaRegistry>();
        builder.Services.AddGrpc()
            .AddServiceOptions<RaftTransportGrpcService>(options => options.Interceptors.Add<PeerCertificateRequiredInterceptor>());

        var app = builder.Build();
        app.Use(async (context, next) => {
            context.Connection.ClientCertificate = clientCertificate;
            await next();
        });
        app.MapGrpcService<RaftTransportGrpcService>();
        await app.StartAsync();
        return app;
    }

    [Fact]
    public async Task RequestVote_without_a_peer_certificate_is_rejected_as_Unauthenticated() {
        await using var host = await StartHostAsync(clientCertificate: null);
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RaftTransport.RaftTransportClient(channel);

        var act = () => client.RequestVoteAsync(new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-1" }).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.Unauthenticated);
    }

    [Fact]
    public async Task RequestVote_with_a_peer_certificate_reaches_the_services_own_logic() {
        var (_, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var leaf = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-2");
        await using var host = await StartHostAsync(leaf);
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RaftTransport.RaftTransportClient(channel);

        var act = () => client.RequestVoteAsync(new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-1" }).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.NotFound,
            "the interceptor let the call through — the service's own Resolve throws NotFound for an unregistered group");
    }
}
