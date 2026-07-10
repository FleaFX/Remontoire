using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Remontoire.Client.V1;

namespace Remontoire.Client;

/// <summary>
/// A connection to one Remontoire group — both producer and consumer, sharing one
/// <see cref="LeaderAddressCache"/> so a successful call on either side benefits every later
/// call on the same group. One gRPC channel per address ever needed, opened lazily and reused for
/// the connection's lifetime: <see cref="RemontoireClientOptions.GroupMemberAddresses"/> seeds it,
/// but a redirect hint the server sends is trusted the same way even when it points somewhere
/// this connection wasn't originally configured with — the hint comes from the same authoritative
/// source (the server's own peer configuration) either way.
/// </summary>
public sealed class RemontoireConnection : IRemontoireProducer, IRemontoireConsumer, IDisposable {
    readonly RemontoireClientOptions _options;
    readonly LeaderAddressCache _leaderCache = new();
    readonly ConcurrentDictionary<Uri, (GrpcChannel Channel, RemontoireClient.RemontoireClientClient Client)> _clients = new();

    // Without mTLS (a later phase), member addresses are plain http:// — .NET's HTTP/2 client
    // otherwise refuses cleartext HTTP/2 outright. Process-wide and idempotent, so setting it
    // here means it can never be silently missed, the same reasoning RaftGrpcTransport uses.
    static RemontoireConnection() => AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

    /// <summary>
    /// Eagerly opens one channel per address in <see cref="RemontoireClientOptions.GroupMemberAddresses"/>.
    /// </summary>
    public RemontoireConnection(RemontoireClientOptions options) {
        _options = options;
        foreach (var address in options.GroupMemberAddresses)
            ClientFor(address);
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
        // leader change, or the node dying outright (an RpcException out of MoveNext, caught
        // separately below since a try surrounding a yield may not have a catch clause), simply
        // breaks the stream — this loop treats both exactly like an initial NotLeader: reconnect
        // via the same redirect logic, not a separate protocol case.
        while (!cancellationToken.IsCancellationRequested) {
            var address = _leaderCache.Get(groupId) ?? RandomMemberAddress();
            var call = ClientFor(address).Consume(new ConsumeRequest { StreamName = streamName, ConsumerGroup = consumerGroup }, cancellationToken: cancellationToken);

            // Set whenever a fresh connection attempt is pointless without backing off first —
            // an unreachable node, or a NotLeader with no hint (an election is in progress and
            // the cache can't be trusted either way). Both get the exact same invalidate-and-wait
            // treatment CallWithRedirectAsync gives its own hintless-redirect case, so a dead or
            // still-electing cached address is never retried in a tight, CPU-burning loop.
            var needsBackoff = false;
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
            }
        }
    }

    // Internal (not private): a test-only seam letting CallWithRedirectAsyncTests exercise the
    // retry/redirect logic with fake delegates, no real gRPC channel needed.
    internal async Task<TReply> CallWithRedirectAsync<TReply>(string groupId, Func<Uri, Task<TReply>> call, Func<TReply, NotLeader?> extractNotLeader) {
        var address = _leaderCache.Get(groupId) ?? RandomMemberAddress();

        for (var attempt = 0; attempt < _options.MaxRedirectAttempts; attempt++) {
            TReply reply;
            try {
                reply = await call(address);
            } catch (RpcException) {
                // The cached or just-tried address is genuinely unreachable (a crashed node, a
                // network partition) — not merely "not currently leader". Treated the same as a
                // redirect with no hint: the cache can no longer be trusted, wait briefly, try a
                // random other member.
                await Task.Delay(_options.RedirectRetryDelay);
                _leaderCache.Invalidate(groupId);
                address = RandomMemberAddress();
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

    RemontoireClient.RemontoireClientClient ClientFor(Uri address) =>
        _clients.GetOrAdd(address, static a => {
            var channel = GrpcChannel.ForAddress(a);
            return (channel, new RemontoireClient.RemontoireClientClient(channel));
        }).Client;

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
        foreach (var (channel, _) in _clients.Values)
            channel.Dispose();
    }
}
