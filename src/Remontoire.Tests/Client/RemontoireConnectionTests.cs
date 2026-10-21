using FluentAssertions;

namespace Remontoire.Client;

public class RemontoireConnectionTests {
    [Fact]
    public void ConfigureHttpHandler_is_called_on_a_fresh_handler_for_the_seed_channel() {
        var handlers = new List<SocketsHttpHandler>();
        using var connection = new RemontoireConnection(new RemontoireClientOptions {
            MetaGroupSeedAddresses = [new Uri("http://127.0.0.1:1")],
            ConfigureHttpHandler = handlers.Add,
        });

        // No server needed: the watcher fails against the unreachable seed in the background, and
        // Dispose at the end of this test stops it — the same pattern CallWithRedirectAsyncTests
        // already uses. Only the seed channel exists at construction time, so exactly one handler.
        handlers.Should().ContainSingle();
    }
}
