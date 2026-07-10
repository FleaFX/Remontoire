using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Net.Client;
using Remontoire.Client.V1;

namespace Remontoire.Client;

/// <summary>
/// A connection to one Remontoire group — both producer and consumer, sharing one
/// <see cref="LeaderAddressCache"/> so a successful call on either side benefits every later
/// call on the same group. One gRPC channel per configured member address, built once and reused
/// for the connection's lifetime.
/// </summary>
public sealed class RemontoireConnection : IRemontoireProducer, IRemontoireConsumer, IDisposable {
    readonly RemontoireClientOptions _options;
    readonly LeaderAddressCache _leaderCache = new();
    readonly Dictionary<Uri, GrpcChannel> _channels;
    readonly Dictionary<Uri, RemontoireClient.RemontoireClientClient> _clients;

    // Without mTLS (a later phase), member addresses are plain http:// — .NET's HTTP/2 client
    // otherwise refuses cleartext HTTP/2 outright. Process-wide and idempotent, so setting it
    // here means it can never be silently missed, the same reasoning RaftGrpcTransport uses.
    static RemontoireConnection() => AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

    /// <summary>
    /// Eagerly opens one channel per address in <see cref="RemontoireClientOptions.GroupMemberAddresses"/>.
    /// </summary>
    public RemontoireConnection(RemontoireClientOptions options) {
        _options = options;
        _channels = options.GroupMemberAddresses.ToDictionary(address => address, GrpcChannel.ForAddress);
        _clients = _channels.ToDictionary(pair => pair.Key, pair => new RemontoireClient.RemontoireClientClient(pair.Value));
    }

    /// <inheritdoc />
    public async Task<PublishResult> PublishAsync(string streamName, string partitionKey, ReadOnlyMemory<byte> payload, IReadOnlyDictionary<string, string>? headers = null, CancellationToken cancellationToken = default) {
        var groupId = ResolveGroupId(streamName);

        var request = new PublishRequest { StreamName = streamName, PartitionKey = ByteString.CopyFromUtf8(partitionKey), Payload = ByteString.CopyFrom(payload.Span) };
        if (headers is not null)
            request.Headers.Add(headers.Select(header => new MessageHeader { Key = ByteString.CopyFromUtf8(header.Key), Value = ByteString.CopyFromUtf8(header.Value) }));

        var reply = await CallWithRedirectAsync(
            groupId, address => ClientFor(address).PublishAsync(request, cancellationToken: cancellationToken).ResponseAsync, reply => reply.NotLeader);

        return new PublishResult(reply.Success.ShardId, (long)reply.Success.Offset, ToDateTimeOffset(reply.Success.IngestedAtMicros));
    }

    /// <inheritdoc />
    public async Task AckAsync(string streamName, string consumerGroup, int shardId, long offset, CancellationToken cancellationToken = default) {
        var groupId = ResolveGroupId(streamName);
        var request = new AckRequest { StreamName = streamName, ConsumerGroup = consumerGroup, ShardId = shardId, Offsets = { (ulong)offset } };

        await CallWithRedirectAsync(
            groupId, address => ClientFor(address).AckAsync(request, cancellationToken: cancellationToken).ResponseAsync, reply => reply.NotLeader);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RemontoireMessage> ConsumeAsync(
        string streamName, string consumerGroup, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        var groupId = ResolveGroupId(streamName);

        // NotLeader can only ever appear as the FIRST reply on the stream — once the server
        // starts streaming, it was, at that moment, the correct leader by definition. A later
        // leader change simply breaks the stream, which this loop treats exactly like an initial
        // NotLeader: reconnect via the same redirect logic, not a separate protocol case.
        while (!cancellationToken.IsCancellationRequested) {
            var address = _leaderCache.Get(groupId) ?? RandomMemberAddress();
            using var call = ClientFor(address).Consume(new ConsumeRequest { StreamName = streamName, ConsumerGroup = consumerGroup }, cancellationToken: cancellationToken);

            while (await call.ResponseStream.MoveNext(cancellationToken)) {
                var reply = call.ResponseStream.Current;
                if (reply.NotLeader is { } notLeader) {
                    if (notLeader.HasLeaderAddress)
                        _leaderCache.Update(groupId, new Uri(notLeader.LeaderAddress));
                    break; // reconnect to the (possibly new) leader
                }

                yield return ToMessage(reply.Message);
            }
        }
    }

    // Internal (not private): a test-only seam letting CallWithRedirectAsyncTests exercise the
    // retry/redirect logic with fake delegates, no real gRPC channel needed.
    internal async Task<TReply> CallWithRedirectAsync<TReply>(string groupId, Func<Uri, Task<TReply>> call, Func<TReply, NotLeader?> extractNotLeader) {
        var address = _leaderCache.Get(groupId) ?? RandomMemberAddress();

        for (var attempt = 0; attempt < _options.MaxRedirectAttempts; attempt++) {
            var reply = await call(address);
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
                address = RandomMemberAddress();
            }
        }

        throw new RemontoireUnavailableException(groupId, _options.MaxRedirectAttempts);
    }

    string ResolveGroupId(string streamName) =>
        _options.StreamGroupIds.TryGetValue(streamName, out var groupId)
            ? groupId
            : throw new ArgumentException($"Unknown stream '{streamName}'.", nameof(streamName));

    Uri RandomMemberAddress() => _options.GroupMemberAddresses[Random.Shared.Next(_options.GroupMemberAddresses.Count)];

    RemontoireClient.RemontoireClientClient ClientFor(Uri address) => _clients.TryGetValue(address, out var client)
        ? client
        : throw new InvalidOperationException($"'{address}' is not a configured group member address.");

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
    /// Disposes every underlying channel.
    /// </summary>
    public void Dispose() {
        foreach (var channel in _channels.Values)
            channel.Dispose();
    }
}
