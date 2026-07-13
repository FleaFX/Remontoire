using System.Net;
using FluentAssertions;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Remontoire.Client.V1;
using Remontoire.Raft.Grpc;
using Remontoire.Server;
using Remontoire.Server.Grpc;
using Remontoire.Sharding;

namespace Remontoire.Security;

// Laag-4 smoke test for the interceptor + Program.cs wiring (fase 8, subtraject A, step 3) — not
// the full authorization scenario matrix yet (RemontoireAuthorizationInterceptorTests covers that
// separately, step 4), just the one thing this step adds: an unauthenticated call is rejected
// before it ever reaches RemontoireClientGrpcService's own logic.
public class RemontoireAuthenticationInterceptorTests {
    static async Task<WebApplication> StartHostAsync() {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

        builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options => options.RequireHttpsMetadata = false);
        builder.Services.AddAuthorization();

        builder.Services.AddSingleton<RaftReplicaRegistry>();
        builder.Services.AddSingleton<MessagingGroupRegistry>();
        builder.Services.AddSingleton<LeaderAddressDirectory>();
        builder.Services.AddSingleton<ShardAssignmentTable>();
        builder.Services.AddSingleton<MigrationAdmissionGate>();

        builder.Services.AddGrpc()
            .AddServiceOptions<RemontoireClientGrpcService>(options => options.Interceptors.Add<RemontoireAuthenticationInterceptor>());

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapGrpcService<RemontoireClientGrpcService>();
        await app.StartAsync();
        return app;
    }

    [Fact]
    public async Task Publish_without_a_bearer_token_is_rejected_as_Unauthenticated() {
        await using var host = await StartHostAsync();
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);

        var act = () => client.PublishAsync(new PublishRequest {
            StreamName = "orders", PartitionKey = ByteString.CopyFromUtf8("key-1"), Payload = ByteString.CopyFromUtf8("hello"),
        }).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.Unauthenticated);
    }
}
