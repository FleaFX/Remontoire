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
using Remontoire.Engine;
using Remontoire.Messaging;
using Remontoire.Raft;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;
using Remontoire.Storage;
using Remontoire.Tests;

namespace Remontoire.Server;

// Real-network exit-criterion coverage for the admin surface — every earlier step tested only
// extracted, pure logic, since the RPC methods themselves need a real ServerCallContext (no fake
// precedent in this codebase, see RemontoireClientGrpcServiceTests's own remarks). This is where
// that deferred coverage lands: real Kestrel host, real JWT, authorized by the host with
// RequireAuthorization on the mapped endpoint, a real single-node meta-group plus two real
// single-node data groups (group-1/group-2), all in-process.
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
                // well-known claim types like "role" into long XML-schema URIs, and the "operator"
                // policy's claim check would then never match, no matter what the token carries.
                options.MapInboundClaims = false;
                options.TokenValidationParameters = new TokenValidationParameters {
                    ValidateIssuer = false, ValidateAudience = false, IssuerSigningKey = SigningKey,
                    NameClaimType = "client_id",
                };
            });
        builder.Services.AddAuthorization(options => options.AddPolicy("operator", policy => policy.RequireClaim("role", "operator")));

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
        builder.Services.AddSingleton(TestNodes.Node(raftRegistry, messagingRegistry, table, journal));
        builder.Services.AddSingleton(new RemontoireNodeInfo(HostsMetaGroupReplica: true));
        builder.Services.AddSingleton<NodeAdmin>();

        var metaReplica = await StartSingleNodeReplicaAsync("__meta__", "meta-node");
        raftRegistry.Register(metaReplica);
        var metaApplier = new ShardAssignmentTableApplier(metaReplica, table, journal);

        // A declaration places a new stream on a registered data group — the harness registers both.
        await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [],
            MetaLogRecord.Encode(new RegisterGroup(GroupOne, [new ShardGroupMember("node-1", new Uri("http://node-1.invalid/"))]))));
        await metaReplica.ProposeAsync(new AppendRequest(Array.Empty<byte>(), [],
            MetaLogRecord.Encode(new RegisterGroup(GroupTwo, [new ShardGroupMember("node-2", new Uri("http://node-2.invalid/"))]))));
        (await ConditionPoller.WaitUntilAsync(() => journal.Snapshot().Records.Count >= 2, TimeSpan.FromSeconds(5))).Should().BeTrue();

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

        builder.Services.AddGrpc();

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapRemontoireAdmin().RequireAuthorization("operator");
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

    static EnsureStreamRequest BootstrapRequest() => new() {
        StreamName = StreamName, MaxRetention = Duration.FromTimeSpan(TimeSpan.FromDays(30)),
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
        public async Task EnsureStream_with_no_token_is_rejected_as_Unauthenticated() {
            await using var harness = await StartHostAsync();
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);

            var act = () => client.EnsureStreamAsync(BootstrapRequest()).ResponseAsync;

            var exception = await act.Should().ThrowAsync<RpcException>();
            exception.Which.StatusCode.Should().Be(StatusCode.Unauthenticated,
                "the host's authorization middleware challenges an anonymous call (HTTP 401)");
        }

        [Fact]
        public async Task EnsureStream_with_a_valid_token_but_no_operator_role_is_rejected_as_PermissionDenied() {
            await using var harness = await StartHostAsync();
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);
            var token = CreateToken(new Claim("client_id", "someone"));

            var act = () => client.EnsureStreamAsync(BootstrapRequest(), BearerHeader(token)).ResponseAsync;

            var exception = await act.Should().ThrowAsync<RpcException>();
            exception.Which.StatusCode.Should().Be(StatusCode.PermissionDenied);
        }

        [Fact]
        public async Task EnsureStream_with_the_operator_role_reaches_the_services_own_logic() {
            await using var harness = await StartHostAsync();
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);
            var token = CreateToken(new Claim("client_id", "operator-1"), new Claim("role", "operator"));

            var act = () => client.EnsureStreamAsync(BootstrapRequest(), BearerHeader(token)).ResponseAsync;

            await act.Should().NotThrowAsync();
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
        public async Task Bootstraps_a_stream_reshards_it_sets_policy_and_shows_it_all_in_the_audit_trail() {
            await using var harness = await StartHostAsync();
            using var channel = GrpcChannel.ForAddress(harness.Host.Urls.First());
            var client = new RemontoireAdmin.RemontoireAdminClient(channel);
            var header = BearerHeader(CreateToken(new Claim("client_id", "operator-1"), new Claim("role", "operator")));

            // The placement picks group-1: both registered groups serve no shards yet, and group-1
            // is ordinally the smallest.
            await client.EnsureStreamAsync(BootstrapRequest(), header);

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

            await client.EnsureConsumerGroupAsync(new EnsureConsumerGroupRequest {
                StreamName = StreamName, ConsumerGroup = "billing", AckMode = AckModeProto.Checkpoint, Mandatory = true,
            }, header);
            // Ensure… waits for this node's own journal, and the applier applies a record to the
            // table before appending it there — so the table shows the policy right away.
            harness.Table.GetConsumerGroupPolicy(StreamName, "billing").Mode.Should().Be(AckMode.Checkpoint);

            // RegisterGroup(2, harness) -> CreateStream(1) -> MigrationStarted+Cutover(2, bootstrap) -> MigrationStarted(1, StartReshard)
            // -> Cutover(1, the real one) -> MigrationCompleted(1) -> DeclareConsumerGroup(1) = 9.
            using var listCall = client.ListMetaLogRecords(new ListMetaLogRecordsRequest { FromVersion = 0 }, header);
            var records = await ReadRecordsAsync(listCall, count: 9);

            records.Should().Contain(r => r.RecordType == "CreateStream" && r.ProposedBy == "operator-1",
                "NodeAdmin proposes with the caller's name, which ProposedBy resolves from User.Identity.Name (NameClaimType client_id)");
            records.Should().Contain(r => r.RecordType == "DeclareConsumerGroup" && r.ProposedBy == "operator-1");
            records.Count(r => r.RecordType == "MigrationStarted").Should().Be(2);
            records.Should().Contain(r => r.RecordType == "MigrationStarted" && r.ProposedBy == "operator-1",
                "the bootstrap's own MigrationStarted, proposed via NodeAdmin, names the operator");
            records.Should().Contain(r => r.RecordType == "MigrationStarted" && r.ProposedBy == "",
                "StartReshard proposes via ReshardOrchestrator, which never sets a proposed-by header");
            records.Should().Contain(r => r.RecordType == "MigrationCompleted" && r.ProposedBy == "");
            records.Count(r => r.RecordType == "Cutover").Should().Be(2, "one from the bootstrap, one from the real reshard");
        }
    }
}
