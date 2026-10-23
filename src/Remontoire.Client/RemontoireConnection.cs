using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Remontoire.Admin.V1;
using Remontoire.Client.V1;
using Remontoire.Meta.V1;
using Remontoire.Sharding;
using Duration = Google.Protobuf.WellKnownTypes.Duration;
using NotLeader = Remontoire.Client.V1.NotLeader;

namespace Remontoire.Client;

/// <summary>
/// A connection to Remontoire — producer, consumer and admin, sharing one <see cref="LeaderAddressCache"/>
/// so a successful call on either side benefits every later call on the same group. One gRPC
/// channel per address ever needed, opened lazily and reused for the connection's lifetime. A
/// stream's virtual shard and that shard's current physical group are never configured directly:
/// a <see cref="ShardAssignmentWatcher"/>, bootstrapped from <see cref="RemontoireClientOptions.MetaGroupSeedAddresses"/>,
/// keeps a local <see cref="ShardAssignmentTable"/> fresh, and every call resolves against it —
/// a redirect hint the server sends is trusted the same way even when it points somewhere this
/// connection has never talked to before, the same authoritative source either way. Declarations
/// go to the admin endpoint of the seeds; descriptions come from the local table.
/// </summary>
public sealed class RemontoireConnection : IRemontoireProducer, IRemontoireConsumer, IRemontoireAdmin, IDisposable {
    // The well-known id of the meta group, for the exceptions of the admin calls.
    const string MetaGroupId = "__meta__";
    static readonly TimeSpan DeclarationVisibleTimeout = TimeSpan.FromSeconds(10);

    readonly RemontoireClientOptions _options;
    readonly LeaderAddressCache _leaderCache = new();
    readonly ConcurrentDictionary<Uri, GrpcChannel> _channels = new();
    readonly ShardAssignmentTable _table = new();
    readonly ShardAssignmentWatcher _watcher;
    readonly GrpcChannel _seedChannel;
    bool _disposed;

    /// <summary>
    /// Starts a <see cref="ShardAssignmentWatcher"/> against the first of
    /// <see cref="RemontoireClientOptions.MetaGroupSeedAddresses"/> immediately.
    /// </summary>
    public RemontoireConnection(RemontoireClientOptions options) {
        _options = options;
        _seedChannel = CreateChannel(options.MetaGroupSeedAddresses[0]);
        _watcher = new ShardAssignmentWatcher(new ShardAssignmentMeta.ShardAssignmentMetaClient(_seedChannel), _table);
    }

    // A fresh SocketsHttpHandler per channel — never shared, since GrpcChannelOptions.DisposeHttpClient
    // ties each handler's lifetime to its own channel's Dispose(); sharing one across multiple
    // channels would make disposing the first channel tear down every other channel's handler too.
    GrpcChannel CreateChannel(Uri address) {
        var channelOptions = new GrpcChannelOptions();

        if (_options.ConfigureHttpHandler is { } configureHttpHandler) {
            var handler = new SocketsHttpHandler();
            configureHttpHandler(handler);
            channelOptions.HttpHandler = handler;
            channelOptions.DisposeHttpClient = true;
        }

        var callCredentials = BuildCallCredentials(_options.GetAccessTokenAsync);
        if (callCredentials is not null) {
            var transportCredentials = address.Scheme == Uri.UriSchemeHttps ? new SslCredentials() : ChannelCredentials.Insecure;
            channelOptions.Credentials = ChannelCredentials.Create(transportCredentials, callCredentials);

            // grpc-dotnet refuses CallCredentials on a plain http:// channel unless this is set
            // explicitly — a host without TLS (typically development) that still requires a
            // bearer token.
            if (address.Scheme != Uri.UriSchemeHttps)
                channelOptions.UnsafeUseInsecureChannelCallCredentials = true;
        }

        return GrpcChannel.ForAddress(address, channelOptions);
    }

    static CallCredentials? BuildCallCredentials(Func<CancellationToken, Task<string>>? getAccessTokenAsync) =>
        getAccessTokenAsync is null
            ? null
            : CallCredentials.FromInterceptor(async (_, metadata) => {
                var token = await getAccessTokenAsync(CancellationToken.None);
                metadata.Add("Authorization", $"Bearer {token}");
            });

    /// <inheritdoc />
    public async Task<PublishResult> PublishAsync(string streamName, string partitionKey, ReadOnlyMemory<byte> payload, IReadOnlyDictionary<string, string>? headers = null, CancellationToken cancellationToken = default) {
        var partitionKeyBytes = ByteString.CopyFromUtf8(partitionKey);
        var virtualShardIndex = ResolveVirtualShardIndex(streamName, partitionKeyBytes.Span);
        var (groupId, memberAddresses) = ResolveGroup(streamName, virtualShardIndex);

        var request = new PublishRequest { StreamName = streamName, PartitionKey = partitionKeyBytes, Payload = ByteString.CopyFrom(payload.Span) };
        if (headers is not null)
            request.Headers.Add(headers.Select(header => new MessageHeader { Key = ByteString.CopyFromUtf8(header.Key), Value = ByteString.CopyFromUtf8(header.Value) }));

        var reply = await CallWithRedirectAsync(
            groupId, memberAddresses, address => ClientFor(address).PublishAsync(request, cancellationToken: cancellationToken).ResponseAsync,
            reply => reply.NotLeader, reply => reply.ShardMigrating is not null);

        return new PublishResult(reply.Success.ShardId, (long)reply.Success.Offset, ToDateTimeOffset(reply.Success.IngestedAtMicros));
    }

    /// <inheritdoc />
    public async Task AckAsync(string streamName, string consumerGroup, int shardId, long offset, CancellationToken cancellationToken = default) {
        RequireConsumerGroup(streamName, consumerGroup);
        // No partition key travels with an ack — virtual shard 0 always, same limitation Consume
        // has, until multi-vshard consumption's own wire contract is settled.
        var (groupId, memberAddresses) = ResolveGroup(streamName, virtualShardIndex: 0);
        var request = new AckRequest { StreamName = streamName, ConsumerGroup = consumerGroup, ShardId = shardId, Offsets = { (ulong)offset } };

        await CallWithRedirectAsync(
            groupId, memberAddresses, address => ClientFor(address).AckAsync(request, cancellationToken: cancellationToken).ResponseAsync,
            reply => reply.NotLeader, reply => reply.ShardMigrating is not null);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RemontoireMessage> ConsumeAsync(
        string streamName, string consumerGroup, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        RequireConsumerGroup(streamName, consumerGroup);
        var (groupId, memberAddresses) = ResolveGroup(streamName, virtualShardIndex: 0);

        // NotLeader can only ever appear as the FIRST reply on the stream — once the server
        // starts streaming, it was, at that moment, the correct leader by definition. A later
        // leader change, or the node dying outright (an RpcException out of MoveNext, caught
        // separately below since a try surrounding a yield may not have a catch clause), simply
        // breaks the stream — this loop treats both exactly like an initial NotLeader: reconnect
        // via the same redirect logic, not a separate protocol case.
        while (!cancellationToken.IsCancellationRequested) {
            var address = _leaderCache.Get(groupId) ?? RandomMemberAddress(memberAddresses);
            var call = ClientFor(address).Consume(new ConsumeRequest { StreamName = streamName, ConsumerGroup = consumerGroup }, cancellationToken: cancellationToken);

            // Set whenever a fresh connection attempt is pointless without backing off first —
            // an unreachable node, or a NotLeader with no hint (an election is in progress and
            // the cache can't be trusted either way). Both get the exact same invalidate-and-wait
            // treatment CallWithRedirectAsync gives its own hintless-redirect case, so a dead or
            // still-electing cached address is never retried in a tight, CPU-burning loop.
            var needsBackoff = false;

            // Set for a ShardMigrating reply — a retryable contention signal, not a redirect: the
            // contacted node IS the leader, just briefly pausing admission for this shard during
            // a reshard cutover. Waits, then reconnects to the SAME (now-confirmed) address —
            // never invalidated, unlike needsBackoff's cache-can't-be-trusted case above.
            var retrySameAddress = false;
            try {
                while (true) {
                    bool hasNext;
                    try {
                        hasNext = await call.ResponseStream.MoveNext(cancellationToken);
                    } catch (RpcException) {
                        needsBackoff = true; // the node is unreachable — reconnect exactly like a stream that just ended
                        break;
                    }

                    if (!hasNext)
                        break;

                    var reply = call.ResponseStream.Current;
                    if (reply.ShardMigrating is not null) {
                        _leaderCache.Update(groupId, address); // hint-free confirmation this address is the real leader
                        retrySameAddress = true;
                        break;
                    }

                    if (reply.NotLeader is { } notLeader) {
                        if (notLeader.HasLeaderAddress)
                            _leaderCache.Update(groupId, new Uri(notLeader.LeaderAddress));
                        else
                            needsBackoff = true; // no hint — an election is in progress
                        break; // reconnect to the (possibly new) leader
                    }

                    yield return ToMessage(reply.Message);
                }
            } finally {
                call.Dispose();
            }

            if (needsBackoff) {
                _leaderCache.Invalidate(groupId);
                await Task.Delay(_options.RedirectRetryDelay, cancellationToken);
            } else if (retrySameAddress) {
                await Task.Delay(_options.RedirectRetryDelay, cancellationToken);
            }
        }
    }

    /// <inheritdoc />
    public async Task EnsureStreamAsync(string streamName, StreamOptions? options = null, CancellationToken cancellationToken = default) {
        options ??= new StreamOptions();
        var request = new EnsureStreamRequest { StreamName = streamName, MaxRetention = Duration.FromTimeSpan(options.MaxRetention) };
        if (options.MaxSizeBytesPerVirtualShard is { } maxSize)
            request.MaxSizeBytesPerVirtualShard = maxSize;
        if (options.CheckpointInterval is { } interval)
            request.CheckpointInterval = Duration.FromTimeSpan(interval);
        if (options.CheckpointOffsetCount is { } offsetCount)
            request.CheckpointOffsetCount = offsetCount;

        await CallAdminAsync(address => AdminFor(address).EnsureStreamAsync(request, cancellationToken: cancellationToken).ResponseAsync, cancellationToken);
        await WaitUntilVisibleAsync(() => _table.TryGetAssignment(streamName, 0, out _) && _table.DescribeStream(streamName) is { } stream
            && stream.MaxRetention == options.MaxRetention && stream.MaxSizeBytesPerVirtualShard == options.MaxSizeBytesPerVirtualShard
            && stream.CheckpointInterval == options.CheckpointInterval && stream.CheckpointOffsetCount == options.CheckpointOffsetCount, cancellationToken);
    }

    /// <inheritdoc />
    public async Task EnsureConsumerGroupAsync(string streamName, string consumerGroup, ConsumerGroupOptions? options = null, CancellationToken cancellationToken = default) {
        options ??= new ConsumerGroupOptions();
        var request = new EnsureConsumerGroupRequest {
            StreamName = streamName, ConsumerGroup = consumerGroup, Mandatory = options.Mandatory,
            AckMode = options.AckMode == AckMode.Checkpoint ? AckModeProto.Checkpoint : AckModeProto.Strict,
        };

        await CallAdminAsync(address => AdminFor(address).EnsureConsumerGroupAsync(request, cancellationToken: cancellationToken).ResponseAsync, cancellationToken);
        await WaitUntilVisibleAsync(() => _table.TryGetConsumerGroupPolicy(streamName, consumerGroup, out var policy)
            && policy.Mode == options.AckMode && policy.Mandatory == options.Mandatory, cancellationToken);
    }

    /// <inheritdoc />
    public Task<StreamDescription?> GetStreamAsync(string streamName, CancellationToken cancellationToken = default) =>
        Task.FromResult(_table.DescribeStream(streamName));

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamDescription>> ListStreamsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_table.DescribeStreams());

    // Any seed with a meta-group replica serves a declaration and forwards it to the leader itself;
    // one that is down, or has no replica, sends this call to the next seed.
    async Task CallAdminAsync(Func<Uri, Task> call, CancellationToken cancellationToken) {
        var seeds = _options.MetaGroupSeedAddresses;
        Exception? lastFailure = null;
        for (var attempt = 0; attempt < _options.MaxRedirectAttempts; attempt++) {
            try {
                await call(seeds[attempt % seeds.Count]);
                return;
            } catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.FailedPrecondition or StatusCode.DeadlineExceeded) {
                lastFailure = ex;
                await Task.Delay(_options.RedirectRetryDelay, cancellationToken);
            } catch (RpcException ex) when (ex.StatusCode == StatusCode.InvalidArgument) {
                throw new ArgumentException(ex.Status.Detail, ex);
            } catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists) {
                throw new RemontoireConflictException(ex.Status.Detail);
            }
        }

        throw new RemontoireUnavailableException(MetaGroupId, _options.MaxRedirectAttempts, lastFailure);
    }

    // A consumer started right after a declaration must find it: wait until this connection's own
    // table shows it.
    async Task WaitUntilVisibleAsync(Func<bool> isVisible, CancellationToken cancellationToken) {
        var deadline = DateTime.UtcNow + DeclarationVisibleTimeout;
        while (!isVisible()) {
            if (DateTime.UtcNow >= deadline)
                throw new RemontoireUnavailableException(MetaGroupId, "The declaration was applied, but this connection's view of the meta log did not catch up in time.");
            await Task.Delay(20, cancellationToken);
        }
    }

    void RequireConsumerGroup(string streamName, string consumerGroup) {
        if (!_table.TryGetConsumerGroupPolicy(streamName, consumerGroup, out _))
            throw new ArgumentException($"No consumer group '{consumerGroup}' is declared on stream '{streamName}'.", nameof(consumerGroup));
    }

    // Internal (not private): a test-only seam letting CallWithRedirectAsyncTests exercise the
    // retry/redirect logic with fake delegates, no real gRPC channel needed.
    internal async Task<TReply> CallWithRedirectAsync<TReply>(
        string groupId, IReadOnlyList<Uri> memberAddresses, Func<Uri, Task<TReply>> call,
        Func<TReply, NotLeader?> extractNotLeader, Func<TReply, bool> isShardMigrating) {
        var address = _leaderCache.Get(groupId) ?? RandomMemberAddress(memberAddresses);
        // Surfaced as RemontoireUnavailableException's own InnerException if every attempt is
        // exhausted — otherwise a caller only ever sees "could not reach a leader," with no way
        // to tell a transport failure (this address is genuinely down/unreachable) apart from a
        // real, sustained "no leader elected" outage.
        Exception? lastFailure = null;

        for (var attempt = 0; attempt < _options.MaxRedirectAttempts; attempt++) {
            TReply reply;
            try {
                reply = await call(address);
            } catch (RpcException ex) {
                // The cached or just-tried address is genuinely unreachable (a crashed node, a
                // network partition) — not merely "not currently leader". Treated the same as a
                // redirect with no hint: the cache can no longer be trusted, wait briefly, try a
                // random other member.
                lastFailure = ex;
                await Task.Delay(_options.RedirectRetryDelay);
                _leaderCache.Invalidate(groupId);
                address = RandomMemberAddress(memberAddresses);
                continue;
            }

            if (isShardMigrating(reply)) {
                // A retryable contention signal, not a redirect — this exact address is the real
                // leader, briefly pausing admission for this one shard during a reshard cutover.
                // Worth remembering (a wrong/stale NotLeader hint self-corrects the same way
                // below; this is the reverse — a hint-free confirmation this address is right).
                _leaderCache.Update(groupId, address);
                await Task.Delay(_options.RedirectRetryDelay);
                continue;
            }

            var notLeader = extractNotLeader(reply);
            if (notLeader is null)
                return reply; // success

            if (notLeader.HasLeaderAddress) {
                address = new Uri(notLeader.LeaderAddress);
                _leaderCache.Update(groupId, address); // a wrong/stale hint simply corrects itself on the next round
            } else {
                // No hint known — an election is in progress. Wait briefly, try a random other member.
                await Task.Delay(_options.RedirectRetryDelay);
                _leaderCache.Invalidate(groupId);
                address = RandomMemberAddress(memberAddresses);
            }
        }

        throw new RemontoireUnavailableException(groupId, _options.MaxRedirectAttempts, lastFailure);
    }

    int ResolveVirtualShardIndex(string streamName, ReadOnlySpan<byte> partitionKey) {
        if (!_table.TryGetStreamConfig(streamName, out var config))
            throw new ArgumentException($"Unknown stream '{streamName}'.", nameof(streamName));

        return ShardRouter.GetVirtualShardIndex(partitionKey, config.VirtualShardCount, config.RoutingAlgorithm);
    }

    (string GroupId, IReadOnlyList<Uri> MemberAddresses) ResolveGroup(string streamName, int virtualShardIndex) {
        if (!_table.TryGetAssignment(streamName, virtualShardIndex, out var assignment) ||
            !_table.TryGetGroup(assignment.GroupId, out var group))
            throw new RemontoireUnavailableException(streamName, 0); // assignment not known (yet) — watcher hasn't caught up

        return (assignment.GroupId, group.Members.Select(member => member.Address).ToArray());
    }

    static Uri RandomMemberAddress(IReadOnlyList<Uri> memberAddresses) => memberAddresses[Random.Shared.Next(memberAddresses.Count)];

    RemontoireClient.RemontoireClientClient ClientFor(Uri address) => new(ChannelFor(address));

    RemontoireAdmin.RemontoireAdminClient AdminFor(Uri address) => new(ChannelFor(address));

    GrpcChannel ChannelFor(Uri address) => _channels.GetOrAdd(address, CreateChannel);

    static RemontoireMessage ToMessage(RemontoireMessageProto proto) => new(
        proto.ShardId,
        (long)proto.Offset,
        proto.PartitionKey.ToStringUtf8(),
        proto.Payload.Memory,
        proto.Headers.ToDictionary(header => header.Key.ToStringUtf8(), header => header.Value.ToStringUtf8()),
        ToDateTimeOffset(proto.IngestedAtMicros)
    );

    static DateTimeOffset ToDateTimeOffset(ulong micros) => DateTimeOffset.UnixEpoch.AddTicks((long)micros * 10);

    /// <summary>
    /// Stops the assignment watcher and disposes every underlying channel. A second call does
    /// nothing.
    /// </summary>
    public void Dispose() {
        // AddClient's container disposes this once per registration it resolves through.
        if (_disposed)
            return;
        _disposed = true;

        _watcher.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _seedChannel.Dispose();

        foreach (var channel in _channels.Values)
            channel.Dispose();
    }
}
