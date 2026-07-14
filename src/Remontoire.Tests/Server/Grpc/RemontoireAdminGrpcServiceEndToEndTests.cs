using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using FluentAssertions;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using Remontoire.Admin.V1;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Security;
using Remontoire.Sharding;
using Remontoire.Storage;
using Remontoire.Tests;

namespace Remontoire.Server.Grpc;

// Real-network exit-criterion coverage for the admin surface — every earlier step tested only
// extracted, pure logic, since the RPC methods themselves need a real ServerCallContext (no fake
// precedent in this codebase, see RemontoireClientGrpcServiceTests's own remarks). This is where
// that deferred coverage lands: real Kestrel host, real JWT, a real single-node
// meta-group plus two real single-node data groups (group-1/group-2), all in-process.
[Collection("RealNetwork")]
public class RemontoireAdminGrpcServiceEndToEndTests {
    const string StreamName = "orders";
    const string GroupOne = "group-1";
    const string GroupTwo = "group-2";

    static readonly SymmetricSecurityKey SigningKey = new(Encoding.UTF8.GetBytes("test-signing-key-at-least-32-bytes-long!!"));

    sealed class Harness : IAsyncDisposable {
        public required WebApplication Host { get; init; }
        public required ShardAssignmentTable Table { get; init; }
        public required RaftReplica MetaReplica { get; init; }
        public required ShardAssignmentTableApplier MetaApplier { get; init; }
        public required RaftReplica GroupOneReplica { get; init; }
        public required (ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator) GroupOneMessaging { get; init; }
        public required AckIndexApplier GroupOneApplier { get; init; }
        public required RaftReplica GroupTwoReplica { get; init; }
        public required (ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator) GroupTwoMessaging { get; init; }
        public required AckIndexApplier GroupTwoApplier { get; init; }
        public required string DirectoryRoot { get; init; }

        public async ValueTask DisposeAsync() {
            await Host.DisposeAsync();
            await MetaApplier.DisposeAsync();
            await MetaReplica.DisposeAsync();
            await GroupOneApplier.DisposeAsync();
            await GroupOneMessaging.RetentionEvaluator.DisposeAsync();
            await GroupOneMessaging.ShardLog.DisposeAsync();
            await GroupOneReplica.DisposeAsync();
            await GroupTwoApplier.DisposeAsync();
            await GroupTwoMessaging.RetentionEvaluator.DisposeAsync();
            await GroupTwoMessaging.ShardLog.DisposeAsync();
            await GroupTwoReplica.DisposeAsync();
            Directory.Delete(DirectoryRoot, recursive: true);
        }
    }

    // registerGroupTwoAsLeader: false lets CopyReshardDataTests exercise a real, unelected TO
    // replica — destination.ProposeAsync throws a genuine NotLeaderException, not a mocked one.
    static async Task<Harness> StartHostAsync(bool registerGroupTwoAsLeader = true) {
        var directoryRoot = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directoryRoot);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.ConfigureLoopbackHttp2());

        builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options => {
                options.RequireHttpsMetadata = false;
                // Load-bearing, not a style choice: without this, ASP.NET Core silently rewrites
                // well-known claim types like "role" into long XML-schema URIs, and
                // RemontoireAuthorizer.HasRole's claim.Type == "role" check would then never
                // match — IsOperator would always be false, no matter what the token carries.
                options.MapInboundClaims = false;
                options.TokenValidationParameters = new TokenValidationParameters {
                    ValidateIssuer = false, ValidateAudience = false, IssuerSigningKey = SigningKey,
                };
            });
        builder.Services.AddAuthorization();
        builder.Services.Configure<RemontoireSecurityOptions>(_ => { });
        builder.Services.AddSingleton<RemontoireAuthorizer>();

        var raftRegistry = new RaftReplicaRegistry();
        var messagingRegistry = new MessagingGroupRegistry();
        var table = new ShardAssignmentTable();
        var journal = new MetaLogJournal();
        var orchestrator = new ReshardOrchestrator(raftRegistry, messagingRegistry, new MigrationAdmissionGate());

        builder.Services.AddSingleton(raftRegistry);
        builder.Services.AddSingleton(messagingRegistry);
        builder.Services.AddSingleton(new LeaderAddressDirectory());
        builder.Services.AddSingleton(table);
        builder.Services.AddSingleton(journal);
        builder.Services.AddSingleton(orchestrator);

        var metaReplica = await StartSingleNodeReplicaAsync("__meta__", "meta-node");
        raftRegistry.Register(metaReplica);
        var metaApplier = new ShardAssignmentTableApplier(metaReplica, table, journal);

        var groupOneReplica = await StartSingleNodeReplicaAsync(GroupOne, "node-1");
        raftRegistry.Register(groupOneReplica);
        var groupOneMessaging = await ComposeMessagingAsync(groupOneReplica, Path.Combine(directoryRoot, GroupOne));
        messagingRegistry.Register(GroupOne, groupOneMessaging.ShardLog, groupOneMessaging.AckIndex, groupOneMessaging.RetentionEvaluator);
        var groupOneApplier = new AckIndexApplier(groupOneMessaging.ShardLog, groupOneMessaging.AckIndex);

        var groupTwoReplica = await StartSingleNodeReplicaAsync(GroupTwo, "node-2", elect: registerGroupTwoAsLeader);
        raftRegistry.Register(groupTwoReplica);
        var groupTwoMessaging = await ComposeMessagingAsync(groupTwoReplica, Path.Combine(directoryRoot, GroupTwo));
        messagingRegistry.Register(GroupTwo, groupTwoMessaging.ShardLog, groupTwoMessaging.AckIndex, groupTwoMessaging.RetentionEvaluator);
        var groupTwoApplier = new AckIndexApplier(groupTwoMessaging.ShardLog, groupTwoMessaging.AckIndex);

        builder.Services.AddGrpc()
            .AddServiceOptions<RemontoireAdminGrpcService>(options => {
                options.Interceptors.Add<RemontoireAuthenticationInterceptor>();
                options.Interceptors.Add<RemontoireAdminAuthorizationInterceptor>();
            });

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapGrpcService<RemontoireAdminGrpcService>();
        await app.StartAsync();

        return new Harness {
            Host = app, Table = table,
            MetaReplica = metaReplica, MetaApplier = metaApplier,
            GroupOneReplica = groupOneReplica, GroupOneMessaging = groupOneMessaging, GroupOneApplier = groupOneApplier,
            GroupTwoReplica = groupTwoReplica, GroupTwoMessaging = groupTwoMessaging, GroupTwoApplier = groupTwoApplier,
            DirectoryRoot = directoryRoot,
        };
    }

    static async Task<RaftReplica> StartSingleNodeReplicaAsync(string groupId, string nodeId, bool elect = true) {
        var config = new RaftReplicaConfig(
            GroupId: groupId, NodeId: nodeId, Peers: [],
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11));

        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config);
        await replica.StartAsync();
        if (elect) {
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // single-node group -> ready leader
            await replica.DrainAsync();
        }
        return replica;
    }

    static async Task<(ShardLog ShardLog, AckIndex AckIndex, RetentionEvaluator RetentionEvaluator)> ComposeMessagingAsync(RaftReplica replica, string directory) {
        Directory.CreateDirectory(directory);
        var ackIndex = new AckIndex();
        var shardLog = await ShardLog.OpenAsync(directory, replica.ReadCommittedAsync,
            compactionPolicy: new CompactionPolicy(MaxAge: null, MaxMergedSegmentBytes: null, GetAckedLowWatermarkAsync: _ => new ValueTask<ulong>(ackIndex.AllGroupsLowWatermark())));
        var retentionEvaluator = new RetentionEvaluator(new RetentionEvaluatorOptions(
            ShardLog: shardLog, AckIndex: ackIndex, IsMandatory: _ => true, GetMaxRetention: () => TimeSpan.MaxValue,
            ForwardToDeadLetterAsync: (_, _) => Task.FromResult(false), IsAdmissionPaused: () => false, IsLeader: () => replica.IsLeader));
        return (shardLog, ackIndex, retentionEvaluator);
    }

    static string CreateToken(params Claim[] claims) {
        var credentials = new SigningCredentials(SigningKey, SecurityAlgorithms.HmacSha256);
        var token = new JwtSecurityToken(claims: claims, expires: DateTime.UtcNow.AddMinutes(5), signingCredentials: credentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    static Metadata BearerHeader(string token) => new() { { "Authorization", $"Bearer {token}" } };

    static CreateStreamRequest BootstrapRequest() => new() {
        StreamName = StreamName, VirtualShardCount = 1, RoutingAlgorithm = RoutingAlgorithmProto.XxHash3V1,
        InitialGroups = { new PhysicalGroupDescriptorProto { GroupId = GroupOne } },
        InitialAssignments = { new VirtualShardAssignmentProto { VirtualShardIndex = 0, GroupId = GroupOne } },
    };

    static async Task<List<MetaLogRecordView>> ReadRecordsAsync(AsyncServerStreamingCall<MetaLogRecordView> call, int count) {
        var records = new List<MetaLogRecordView>();
        for (var i = 0; i < count; i++) {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            (await call.ResponseStream.MoveNext(cts.Token)).Should().BeTrue();
            records.Add(call.ResponseStream.Current);
        }
        return records;
    }

    public class AuthorizationGating {
        [Fact]
        public async Task CreateStream_with_no_token_is_rejected_as_Unauthenticated_before_authorization_ever_runs() {
            await using var harness = await StartHostAsync();
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);

            var act = () => client.CreateStreamAsync(BootstrapRequest()).ResponseAsync;

            var exception = await act.Should().ThrowAsync<RpcException>();
            exception.Which.StatusCode.Should().Be(StatusCode.Unauthenticated,
                "the authentication interceptor is registered before the admin authorization one and must short-circuit first");
        }

        [Fact]
        public async Task CreateStream_with_a_valid_token_but_no_operator_role_is_rejected_as_PermissionDenied() {
            await using var harness = await StartHostAsync();
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);
            var token = CreateToken(new Claim("client_id", "someone"));

            var act = () => client.CreateStreamAsync(BootstrapRequest(), BearerHeader(token)).ResponseAsync;

            var exception = await act.Should().ThrowAsync<RpcException>();
            exception.Which.StatusCode.Should().Be(StatusCode.PermissionDenied);
        }

        [Fact]
        public async Task CreateStream_with_the_operator_role_reaches_the_services_own_logic() {
            await using var harness = await StartHostAsync();
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);
            var token = CreateToken(new Claim("client_id", "operator-1"), new Claim("role", "operator"));

            var reply = await client.CreateStreamAsync(BootstrapRequest(), BearerHeader(token));

            reply.ResultCase.Should().Be(CreateStreamReply.ResultOneofCase.Success);
        }
    }

    public class CopyReshardDataTests {
        [Fact]
        public async Task Translates_a_real_NotLeaderException_from_an_unelected_TO_replica_into_a_stream_redirect() {
            await using var harness = await StartHostAsync(registerGroupTwoAsLeader: false);
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);
            var token = CreateToken(new Claim("client_id", "operator-1"), new Claim("role", "operator"));

            // CopyRecordsAsync's copy loop only ever calls destination.ProposeAsync once it finds
            // an actual record to copy (ReshardOrchestrator.cs) — the FROM group needs at least one
            // committed message, or the NotLeaderException this test targets never gets a chance to
            // be thrown at all.
            var published = await harness.GroupOneReplica.ProposeAsync(new AppendRequest("key-1"u8.ToArray(), [], "hello"u8.ToArray()));
            await ConditionPoller.WaitUntilAsync(() => harness.GroupOneMessaging.ShardLog.TryGet(published.LogicalOffset, out _), TimeSpan.FromSeconds(5));

            using var call = client.CopyReshardData(new CopyReshardDataRequest {
                MigrationId = Guid.NewGuid().ToString(), FromGroupId = GroupOne, ToGroupId = GroupTwo, FromOffset = 0,
            }, BearerHeader(token));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            (await call.ResponseStream.MoveNext(cts.Token)).Should().BeTrue();
            call.ResponseStream.Current.ResultCase.Should().Be(CopyReshardDataProgress.ResultOneofCase.NotLeader);
            call.ResponseStream.Current.NotLeader.GroupId.Should().Be(GroupTwo);
        }
    }

    public class FullExitCriterion {
        [Fact]
        public async Task Bootstraps_a_stream_reshards_it_sets_policy_and_ACL_and_shows_it_all_in_the_audit_trail() {
            await using var harness = await StartHostAsync();
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);
            var header = BearerHeader(CreateToken(new Claim("client_id", "operator-1"), new Claim("role", "operator")));

            (await client.CreateStreamAsync(BootstrapRequest(), header)).ResultCase.Should().Be(CreateStreamReply.ResultOneofCase.Success);

            var migrationId = Guid.NewGuid().ToString();
            (await client.StartReshardAsync(new StartReshardRequest {
                MigrationId = migrationId, StreamName = StreamName, VirtualShardIndex = 0, FromGroupId = GroupOne, ToGroupId = GroupTwo,
            }, header)).ResultCase.Should().Be(StartReshardReply.ResultOneofCase.Success);

            using (var copyCall = client.CopyReshardData(new CopyReshardDataRequest {
                MigrationId = migrationId, FromGroupId = GroupOne, ToGroupId = GroupTwo, FromOffset = 0,
            }, header)) {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                (await copyCall.ResponseStream.MoveNext(cts.Token)).Should().BeTrue();
                copyCall.ResponseStream.Current.ResultCase.Should().Be(CopyReshardDataProgress.ResultOneofCase.Progress);
                copyCall.ResponseStream.Current.Progress.CaughtUp.Should().BeTrue("a freshly created stream has no data to copy");
            }

            (await client.CutoverAsync(new CutoverRequest {
                MigrationId = migrationId, StreamName = StreamName, VirtualShardIndex = 0, FromGroupId = GroupOne, ToGroupId = GroupTwo,
                Timeout = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
            }, header)).ResultCase.Should().Be(CutoverReply.ResultOneofCase.Success);

            harness.Table.TryGetAssignment(StreamName, 0, out var assignment).Should().BeTrue();
            assignment.GroupId.Should().Be(GroupTwo, "Cutover must have flipped routing to the new group, observed via ConditionPoller");

            (await client.CompleteReshardAsync(new CompleteReshardRequest {
                MigrationId = migrationId, StreamName = StreamName, VirtualShardIndex = 0,
            }, header)).ResultCase.Should().Be(CompleteReshardReply.ResultOneofCase.Success);

            (await client.SetConsumerGroupFlagAsync(new SetConsumerGroupFlagRequest {
                StreamName = StreamName, ConsumerGroup = "billing", AckMode = AckModeProto.Checkpoint,
            }, header)).ResultCase.Should().Be(SetConsumerGroupFlagReply.ResultOneofCase.Success);
            // Unlike Cutover, this RPC returns as soon as the meta-group commits — it never waits
            // for ShardAssignmentTableApplier's own, separate background loop to have applied the
            // record locally yet, so the table read right after must poll rather than assume.
            (await ConditionPoller.WaitUntilAsync(
                () => harness.Table.GetConsumerGroupPolicy(StreamName, "billing").Mode == AckMode.Checkpoint, TimeSpan.FromSeconds(5))).Should().BeTrue();

            (await client.ManageAclAsync(new ManageAclRequest {
                ProduceAcl = new ProduceAclChange { Subject = "client-1", StreamName = StreamName, Allowed = true },
            }, header)).ResultCase.Should().Be(ManageAclReply.ResultOneofCase.Success);
            (await ConditionPoller.WaitUntilAsync(() => harness.Table.CanProduce("client-1", StreamName), TimeSpan.FromSeconds(5))).Should().BeTrue();

            // CreateStream(1) -> RegisterGroup(1) -> MigrationStarted+Cutover(2, bootstrap) -> MigrationStarted(1, StartReshard)
            // -> Cutover(1, the real one) -> MigrationCompleted(1) -> SetConsumerGroupAckMode(1) -> SetProduceAcl(1) = 9.
            using var listCall = client.ListMetaLogRecords(new ListMetaLogRecordsRequest { FromVersion = 0 }, header);
            var records = await ReadRecordsAsync(listCall, count: 9);

            records.Should().Contain(r => r.RecordType == "CreateStream" && r.ProposedBy == "operator-1",
                "CreateStream proposes via ToAppendRequest, which sets proposed-by from the authenticated subject");
            records.Should().Contain(r => r.RecordType == "SetProduceAcl" && r.ProposedBy == "operator-1");
            records.Should().Contain(r => r.RecordType == "MigrationStarted" && r.ProposedBy == "",
                "StartReshard/CreateStream's own bootstrap MigrationStarted both propose via ReshardOrchestrator/a shared record type — neither ever sets a proposed-by header");
            records.Should().Contain(r => r.RecordType == "MigrationCompleted" && r.ProposedBy == "");
            records.Count(r => r.RecordType == "Cutover").Should().Be(2, "one from CreateStream's own bootstrap, one from the real reshard");
        }
    }
}
