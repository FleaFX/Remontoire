using FluentAssertions;
using Remontoire.Client.V1;

namespace Remontoire.Client;

public class CallWithRedirectAsyncTests {
    static readonly Uri MemberAddress = new("http://node-1.local:5000");
    static readonly Uri OtherMemberAddress = new("http://node-2.local:5000");

    static RemontoireConnection Connect(int maxRedirectAttempts = 5, params Uri[] members) =>
        new(new RemontoireClientOptions(
            StreamGroupIds: new Dictionary<string, string> { ["stream-1"] = "group-1" },
            GroupMemberAddresses: members.Length > 0 ? members : [MemberAddress],
            MaxRedirectAttempts: maxRedirectAttempts,
            RedirectRetryDelay: TimeSpan.Zero));

    [Fact]
    public async Task Returns_immediately_when_the_first_call_already_succeeds() {
        using var connection = Connect();
        var attempts = 0;

        var reply = await connection.CallWithRedirectAsync<string>("group-1", _ => {
            attempts++;
            return Task.FromResult("ok");
        }, _ => null);

        reply.Should().Be("ok");
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task Retries_against_the_hinted_address_after_a_NotLeader_redirect() {
        using var connection = Connect();
        var addressesCalled = new List<Uri>();

        var reply = await connection.CallWithRedirectAsync<string>("group-1", address => {
            addressesCalled.Add(address);
            return Task.FromResult(addressesCalled.Count == 1 ? "redirect" : "ok");
        }, reply => reply == "redirect" ? new NotLeader { StreamName = "stream-1", LeaderAddress = OtherMemberAddress.ToString() } : null);

        reply.Should().Be("ok");
        addressesCalled.Should().Equal(MemberAddress, OtherMemberAddress);
    }

    [Fact]
    public async Task A_redirect_hint_is_cached_for_the_next_call_on_the_same_group() {
        using var connection = Connect();

        await connection.CallWithRedirectAsync<string>("group-1", address =>
            Task.FromResult(address == OtherMemberAddress ? "ok" : "redirect"),
            reply => reply == "redirect" ? new NotLeader { StreamName = "stream-1", LeaderAddress = OtherMemberAddress.ToString() } : null);

        var addressesCalled = new List<Uri>();
        await connection.CallWithRedirectAsync<string>("group-1", address => {
            addressesCalled.Add(address);
            return Task.FromResult("ok");
        }, _ => null);

        addressesCalled.Should().Equal([OtherMemberAddress], "the cached hint from the first call must be used, not a random member");
    }

    [Fact]
    public async Task Retries_a_random_member_when_the_redirect_carries_no_hint() {
        using var connection = Connect();
        var attempts = 0;

        var reply = await connection.CallWithRedirectAsync<string>("group-1", _ => {
            attempts++;
            return Task.FromResult(attempts == 1 ? "redirect" : "ok");
        }, reply => reply == "redirect" ? new NotLeader { StreamName = "stream-1" } : null); // no LeaderAddress — an election is in progress

        reply.Should().Be("ok");
        attempts.Should().Be(2);
    }

    [Fact]
    public async Task Throws_RemontoireUnavailableException_after_exhausting_every_redirect_attempt() {
        using var connection = Connect(maxRedirectAttempts: 3);

        var act = () => connection.CallWithRedirectAsync<string>("group-1",
            _ => Task.FromResult("redirect"),
            reply => reply == "redirect" ? new NotLeader { StreamName = "stream-1", LeaderAddress = OtherMemberAddress.ToString() } : null);

        var exception = await act.Should().ThrowAsync<RemontoireUnavailableException>();
        exception.Which.GroupId.Should().Be("group-1");
        exception.Which.Attempts.Should().Be(3);
    }
}
