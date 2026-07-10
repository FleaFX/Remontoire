using FluentAssertions;
using Grpc.Core;
using Remontoire.Client.V1;

namespace Remontoire.Client;

public class CallWithRedirectAsyncTests {
    static readonly Uri MemberAddress = new("http://node-1.local:5000");
    static readonly Uri OtherMemberAddress = new("http://node-2.local:5000");
    static readonly IReadOnlyList<Uri> Members = [MemberAddress];

    static RemontoireConnection Connect(int maxRedirectAttempts = 5) =>
        new(new RemontoireClientOptions(
            MetaGroupSeedAddresses: [MemberAddress],
            MaxRedirectAttempts: maxRedirectAttempts,
            RedirectRetryDelay: TimeSpan.Zero));

    static bool NeverShardMigrating(string reply) => false;

    [Fact]
    public async Task Returns_immediately_when_the_first_call_already_succeeds() {
        using var connection = Connect();
        var attempts = 0;

        var reply = await connection.CallWithRedirectAsync<string>("group-1", Members, _ => {
            attempts++;
            return Task.FromResult("ok");
        }, _ => null, NeverShardMigrating);

        reply.Should().Be("ok");
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task Retries_against_the_hinted_address_after_a_NotLeader_redirect() {
        using var connection = Connect();
        var addressesCalled = new List<Uri>();

        var reply = await connection.CallWithRedirectAsync<string>("group-1", Members, address => {
            addressesCalled.Add(address);
            return Task.FromResult(addressesCalled.Count == 1 ? "redirect" : "ok");
        }, reply => reply == "redirect" ? new NotLeader { StreamName = "stream-1", LeaderAddress = OtherMemberAddress.ToString() } : null, NeverShardMigrating);

        reply.Should().Be("ok");
        addressesCalled.Should().Equal(MemberAddress, OtherMemberAddress);
    }

    [Fact]
    public async Task A_redirect_hint_is_cached_for_the_next_call_on_the_same_group() {
        using var connection = Connect();

        await connection.CallWithRedirectAsync<string>("group-1", Members, address =>
            Task.FromResult(address == OtherMemberAddress ? "ok" : "redirect"),
            reply => reply == "redirect" ? new NotLeader { StreamName = "stream-1", LeaderAddress = OtherMemberAddress.ToString() } : null, NeverShardMigrating);

        var addressesCalled = new List<Uri>();
        await connection.CallWithRedirectAsync<string>("group-1", Members, address => {
            addressesCalled.Add(address);
            return Task.FromResult("ok");
        }, _ => null, NeverShardMigrating);

        addressesCalled.Should().Equal([OtherMemberAddress], "the cached hint from the first call must be used, not a random member");
    }

    [Fact]
    public async Task Retries_a_random_member_when_the_redirect_carries_no_hint() {
        using var connection = Connect();
        var attempts = 0;

        var reply = await connection.CallWithRedirectAsync<string>("group-1", Members, _ => {
            attempts++;
            return Task.FromResult(attempts == 1 ? "redirect" : "ok");
        }, reply => reply == "redirect" ? new NotLeader { StreamName = "stream-1" } : null, NeverShardMigrating); // no LeaderAddress — an election is in progress

        reply.Should().Be("ok");
        attempts.Should().Be(2);
    }

    [Fact]
    public async Task Retries_a_random_member_when_the_cached_address_throws_an_RpcException() {
        // A genuinely unreachable node (a crash, a network partition) throws rather than
        // replying — this must be treated exactly like a hintless redirect, not left unhandled.
        using var connection = Connect();
        var attempts = 0;

        var reply = await connection.CallWithRedirectAsync<string>("group-1", Members, _ => {
            attempts++;
            if (attempts == 1)
                throw new RpcException(new Status(StatusCode.Unavailable, "simulated dead node"));

            return Task.FromResult("ok");
        }, _ => null, NeverShardMigrating);

        reply.Should().Be("ok");
        attempts.Should().Be(2);
    }

    [Fact]
    public async Task Throws_RemontoireUnavailableException_after_exhausting_every_redirect_attempt() {
        using var connection = Connect(maxRedirectAttempts: 3);

        var act = () => connection.CallWithRedirectAsync<string>("group-1", Members,
            _ => Task.FromResult("redirect"),
            reply => reply == "redirect" ? new NotLeader { StreamName = "stream-1", LeaderAddress = OtherMemberAddress.ToString() } : null, NeverShardMigrating);

        var exception = await act.Should().ThrowAsync<RemontoireUnavailableException>();
        exception.Which.GroupId.Should().Be("group-1");
        exception.Which.Attempts.Should().Be(3);
    }

    [Fact]
    public async Task Retries_the_same_address_after_a_ShardMigrating_reply() {
        using var connection = Connect();
        var addressesCalled = new List<Uri>();

        var reply = await connection.CallWithRedirectAsync<string>("group-1", Members, address => {
            addressesCalled.Add(address);
            return Task.FromResult(addressesCalled.Count == 1 ? "migrating" : "ok");
        }, _ => null, reply => reply == "migrating");

        reply.Should().Be("ok");
        addressesCalled.Should().Equal([MemberAddress, MemberAddress], "a ShardMigrating reply is a retryable contention signal, not a redirect elsewhere");
    }

    [Fact]
    public async Task A_ShardMigrating_reply_is_cached_as_the_group_leader_for_the_next_call() {
        using var connection = Connect();
        var attempts = 0;
        Uri? firstAddressContacted = null;

        // Whichever of the two candidates the initial random pick lands on replies "migrating"
        // once, then "ok" on retry — the point under test is that address, whichever it was,
        // gets remembered, not any specific one of the two.
        await connection.CallWithRedirectAsync<string>("group-1", [MemberAddress, OtherMemberAddress], address => {
            attempts++;
            firstAddressContacted ??= address;
            return Task.FromResult(attempts == 1 ? "migrating" : "ok");
        }, _ => null, reply => reply == "migrating");

        var addressesCalled = new List<Uri>();
        await connection.CallWithRedirectAsync<string>("group-1", [MemberAddress, OtherMemberAddress], address => {
            addressesCalled.Add(address);
            return Task.FromResult("ok");
        }, _ => null, NeverShardMigrating);

        firstAddressContacted.Should().NotBeNull();
        addressesCalled.Should().Equal([firstAddressContacted!], "the address that answered ShardMigrating is a confirmed leader, worth remembering");
    }

    [Fact]
    public async Task Throws_RemontoireUnavailableException_after_exhausting_every_attempt_against_a_persistently_migrating_shard() {
        using var connection = Connect(maxRedirectAttempts: 3);

        var act = () => connection.CallWithRedirectAsync<string>("group-1", Members,
            _ => Task.FromResult("migrating"), _ => null, reply => reply == "migrating");

        var exception = await act.Should().ThrowAsync<RemontoireUnavailableException>();
        exception.Which.GroupId.Should().Be("group-1");
        exception.Which.Attempts.Should().Be(3);
    }
}
