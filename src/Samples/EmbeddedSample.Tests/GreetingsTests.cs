using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace EmbeddedSample.Tests;

public class GreetingsTests {
    [Fact]
    public async Task A_posted_greeting_is_handled_by_the_embedded_consumer() {
        var dataDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            await using (var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
                    builder.UseSetting("Remontoire:DataDirectory", dataDirectory))) {
                using var client = factory.CreateClient();

                (await client.PostAsync("/greetings?name=Ada", content: null)).StatusCode.Should().Be(HttpStatusCode.Accepted);

                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
                string[]? greetings = null;
                while (DateTime.UtcNow < deadline) {
                    greetings = await client.GetFromJsonAsync<string[]>("/greetings");
                    if (greetings is ["Ada"])
                        break;

                    await Task.Delay(50);
                }

                greetings.Should().Equal("Ada");
                Directory.Exists(Path.Combine(dataDirectory, "groups", "greetings")).Should().BeTrue();
            }
        } finally {
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, recursive: true);
        }
    }
}
