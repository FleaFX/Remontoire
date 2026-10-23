using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Remontoire;

namespace Remontoire.Client;

public class RemontoireClientBuilderExtensionsTests {
    [Fact]
    public async Task AddClient_registers_one_connection_as_producer_and_consumer() {
        var services = new ServiceCollection();
        services.AddRemontoire().AddClient(o => o.MetaGroupSeedAddresses = [new Uri("http://127.0.0.1:1")]);
        await using var provider = services.BuildServiceProvider();

        // No server needed: the watcher fails against the unreachable seed in the background, and
        // disposing the provider stops it — the same pattern RemontoireConnectionTests uses.
        var connection = provider.GetRequiredService<RemontoireConnection>();
        provider.GetRequiredService<IRemontoireProducer>().Should().BeSameAs(connection);
        provider.GetRequiredService<IRemontoireConsumer>().Should().BeSameAs(connection);
        provider.GetRequiredService<IRemontoireAdmin>().Should().BeSameAs(connection);
    }
}
