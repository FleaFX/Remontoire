using FluentAssertions;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Remontoire.Server;

// Regression test for a real pitfall: JsonConsoleFormatter silently drops scopes with no error at
// all unless IncludeScopes is explicitly set to true.
public class JsonConsoleLoggingTests {
    [Fact]
    public void A_log_line_written_inside_a_scope_carries_NodeId_and_ShardGroupId_when_IncludeScopes_is_enabled() {
        var output = CaptureJsonConsoleOutput(includeScopes: true, log: logger => {
            using (logger.BeginScope(new Dictionary<string, object> { ["NodeId"] = "node-1", ["ShardGroupId"] = "group-1" }))
                logger.LogInformation("Something happened.");
        });

        using var document = JsonDocument.Parse(output);
        var root = document.RootElement;
        root.GetProperty("Message").GetString().Should().Be("Something happened.");

        var scope = root.GetProperty("Scopes").EnumerateArray().Should().ContainSingle().Which;
        scope.GetProperty("NodeId").GetString().Should().Be("node-1");
        scope.GetProperty("ShardGroupId").GetString().Should().Be("group-1");
    }

    [Fact]
    public void A_log_line_written_inside_a_scope_carries_no_scope_fields_when_IncludeScopes_is_disabled() {
        var output = CaptureJsonConsoleOutput(includeScopes: false, log: logger => {
            using (logger.BeginScope(new Dictionary<string, object> { ["NodeId"] = "node-1", ["ShardGroupId"] = "group-1" }))
                logger.LogInformation("Something happened.");
        });

        using var document = JsonDocument.Parse(output);
        var root = document.RootElement;
        root.TryGetProperty("Scopes", out _).Should().BeFalse("IncludeScopes defaults to false — scopes are silently absent, not an error");
    }

    static string CaptureJsonConsoleOutput(bool includeScopes, Action<ILogger> log) {
        var originalOut = Console.Out;
        var writer = new StringWriter();
        Console.SetOut(writer);
        try {
            using var factory = LoggerFactory.Create(builder => builder.AddJsonConsole(options => options.IncludeScopes = includeScopes));
            log(factory.CreateLogger("Remontoire.Server.JsonConsoleLoggingTests"));
        } finally {
            Console.SetOut(originalOut);
        }

        return writer.ToString().Trim();
    }
}
