using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Security.Claims;
using System.Text;
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
using Microsoft.IdentityModel.Tokens;
using Remontoire.Client.V1;
using Remontoire.Raft.Grpc;
using Remontoire.Server;
using Remontoire.Server.Grpc;
using Remontoire.Sharding;

namespace Remontoire.Security;

// Laag-4 scenario matrix for RemontoireAuthorizationInterceptor (fase 8, subtraject A, step 4).
// The interceptor rejects before RemontoireClientGrpcService's own logic ever runs, so "authorized"
// here is proven by reaching NotFound (no stream registered in this minimal host) rather than a
// full, working Publish/Ack/Consume — that end-to-end business logic is already covered elsewhere
// (RemontoireGrpcClusterTests); this file is only about the authorization gate itself.
public class RemontoireAuthorizationInterceptorTests {
    const string StreamName = "orders";
    const string ConsumerGroup = "billing";

    static readonly SymmetricSecurityKey SigningKey = new(Encoding.UTF8.GetBytes("test-signing-key-at-least-32-bytes-long!!"));

    static async Task<WebApplication> StartHostAsync(ShardAssignmentTable table) {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

        builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options => {
                options.RequireHttpsMetadata = false;
                options.TokenValidationParameters = new TokenValidationParameters {
                    ValidateIssuer = false, ValidateAudience = false, IssuerSigningKey = SigningKey,
                };
            });
        builder.Services.AddAuthorization();
        builder.Services.Configure<RemontoireSecurityOptions>(_ => { });
        builder.Services.AddSingleton(table);
        builder.Services.AddSingleton<RemontoireAuthorizer>();

        builder.Services.AddSingleton<RaftReplicaRegistry>();
        builder.Services.AddSingleton<MessagingGroupRegistry>();
        builder.Services.AddSingleton<LeaderAddressDirectory>();
        builder.Services.AddSingleton<MigrationAdmissionGate>();

        builder.Services.AddGrpc()
            .AddServiceOptions<RemontoireClientGrpcService>(options => {
                options.Interceptors.Add<RemontoireAuthenticationInterceptor>();
                options.Interceptors.Add<RemontoireAuthorizationInterceptor>();
            });

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapGrpcService<RemontoireClientGrpcService>();
        await app.StartAsync();
        return app;
    }

    static string CreateToken(params Claim[] claims) {
        var credentials = new SigningCredentials(SigningKey, SecurityAlgorithms.HmacSha256);
        var token = new JwtSecurityToken(claims: claims, expires: DateTime.UtcNow.AddMinutes(5), signingCredentials: credentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    static Metadata BearerHeader(string token) => new() { { "Authorization", $"Bearer {token}" } };

    [Fact]
    public async Task Publish_with_no_token_is_rejected_as_Unauthenticated_before_authorization_ever_runs() {
        await using var host = await StartHostAsync(new ShardAssignmentTable());
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);

        var act = () => client.PublishAsync(new PublishRequest {
            StreamName = StreamName, PartitionKey = ByteString.CopyFromUtf8("key-1"), Payload = ByteString.CopyFromUtf8("hello"),
        }).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.Unauthenticated,
            "the authentication interceptor is registered before the authorization one and must short-circuit first");
    }

    [Fact]
    public async Task Publish_with_a_valid_token_but_no_ACL_grant_is_rejected_as_PermissionDenied() {
        await using var host = await StartHostAsync(new ShardAssignmentTable());
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);
        var token = CreateToken(new Claim("client_id", "client-1"));

        var act = () => client.PublishAsync(new PublishRequest {
            StreamName = StreamName, PartitionKey = ByteString.CopyFromUtf8("key-1"), Payload = ByteString.CopyFromUtf8("hello"),
        }, BearerHeader(token)).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.PermissionDenied);
    }

    [Fact]
    public async Task Publish_with_a_valid_token_and_a_matching_ACL_grant_reaches_the_services_own_logic() {
        var table = new ShardAssignmentTable();
        table.Apply(new SetProduceAcl("client-1", StreamName, true));
        await using var host = await StartHostAsync(table);
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);
        var token = CreateToken(new Claim("client_id", "client-1"));

        var act = () => client.PublishAsync(new PublishRequest {
            StreamName = StreamName, PartitionKey = ByteString.CopyFromUtf8("key-1"), Payload = ByteString.CopyFromUtf8("hello"),
        }, BearerHeader(token)).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.NotFound,
            "authorization passed — the call reached RemontoireClientGrpcService's own logic, which has no stream registered in this minimal host");
    }

    [Fact]
    public async Task Publish_with_the_operator_role_but_no_ACL_grant_is_still_rejected_as_PermissionDenied() {
        // The concrete regression test for the role/ACL orthogonality decision: an operator role
        // must never substitute for a missing, explicit ACL grant on the data plane.
        await using var host = await StartHostAsync(new ShardAssignmentTable());
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);
        var token = CreateToken(new Claim("client_id", "client-1"), new Claim("role", "operator"));

        var act = () => client.PublishAsync(new PublishRequest {
            StreamName = StreamName, PartitionKey = ByteString.CopyFromUtf8("key-1"), Payload = ByteString.CopyFromUtf8("hello"),
        }, BearerHeader(token)).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.PermissionDenied);
    }

    [Fact]
    public async Task Ack_with_a_valid_token_and_a_matching_ACL_grant_reaches_the_services_own_logic() {
        var table = new ShardAssignmentTable();
        table.Apply(new SetConsumeAcl("client-1", StreamName, ConsumerGroup, true));
        await using var host = await StartHostAsync(table);
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);
        var token = CreateToken(new Claim("client_id", "client-1"));

        var act = () => client.AckAsync(new Remontoire.Client.V1.AckRequest {
            StreamName = StreamName, ConsumerGroup = ConsumerGroup, ShardId = 0, Offsets = { 0UL },
        }, BearerHeader(token)).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.NotFound);
    }

    [Fact]
    public async Task Ack_with_a_valid_token_but_no_ACL_grant_is_rejected_as_PermissionDenied() {
        await using var host = await StartHostAsync(new ShardAssignmentTable());
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);
        var token = CreateToken(new Claim("client_id", "client-1"));

        var act = () => client.AckAsync(new Remontoire.Client.V1.AckRequest {
            StreamName = StreamName, ConsumerGroup = ConsumerGroup, ShardId = 0, Offsets = { 0UL },
        }, BearerHeader(token)).ResponseAsync;

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.PermissionDenied);
    }

    [Fact]
    public async Task Consume_with_a_valid_token_but_no_ACL_grant_is_rejected_as_PermissionDenied() {
        await using var host = await StartHostAsync(new ShardAssignmentTable());
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);
        var token = CreateToken(new Claim("client_id", "client-1"));

        using var call = client.Consume(new ConsumeRequest { StreamName = StreamName, ConsumerGroup = ConsumerGroup }, BearerHeader(token));
        var act = () => call.ResponseStream.MoveNext();

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.PermissionDenied);
    }

    [Fact]
    public async Task Consume_with_a_valid_token_and_a_matching_ACL_grant_reaches_the_services_own_logic() {
        var table = new ShardAssignmentTable();
        table.Apply(new SetConsumeAcl("client-1", StreamName, ConsumerGroup, true));
        await using var host = await StartHostAsync(table);
        using var channel = GrpcChannel.ForAddress(host.Urls.First());
        var client = new RemontoireClient.RemontoireClientClient(channel);
        var token = CreateToken(new Claim("client_id", "client-1"));

        using var call = client.Consume(new ConsumeRequest { StreamName = StreamName, ConsumerGroup = ConsumerGroup }, BearerHeader(token));
        var act = () => call.ResponseStream.MoveNext();

        var exception = await act.Should().ThrowAsync<RpcException>();
        exception.Which.StatusCode.Should().Be(StatusCode.NotFound,
            "authorization passed — the call reached RemontoireClientGrpcService's own logic, which has no stream registered in this minimal host");
    }
}
