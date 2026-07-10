using FluentAssertions;

namespace Remontoire.Client;

public class LeaderAddressCacheTests {
    [Fact]
    public void Get_returns_null_for_an_unknown_group() {
        var cache = new LeaderAddressCache();

        cache.Get("group-1").Should().BeNull();
    }

    [Fact]
    public void Update_then_Get_returns_the_cached_address() {
        var cache = new LeaderAddressCache();
        var address = new Uri("http://node-1.local:5000");

        cache.Update("group-1", address);

        cache.Get("group-1").Should().Be(address);
    }

    [Fact]
    public void Update_overwrites_a_previously_cached_address() {
        var cache = new LeaderAddressCache();
        cache.Update("group-1", new Uri("http://node-1.local:5000"));

        var latest = new Uri("http://node-2.local:5000");
        cache.Update("group-1", latest);

        cache.Get("group-1").Should().Be(latest);
    }

    [Fact]
    public void Invalidate_clears_the_cached_address() {
        var cache = new LeaderAddressCache();
        cache.Update("group-1", new Uri("http://node-1.local:5000"));

        cache.Invalidate("group-1");

        cache.Get("group-1").Should().BeNull();
    }

    [Fact]
    public void Groups_are_tracked_independently() {
        var cache = new LeaderAddressCache();
        var address = new Uri("http://node-1.local:5000");

        cache.Update("group-1", address);

        cache.Get("group-2").Should().BeNull();
    }
}
