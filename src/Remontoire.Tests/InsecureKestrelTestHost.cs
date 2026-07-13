using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace Remontoire.Tests;

// The repeated "loopback, HTTP/2, no TLS" Kestrel config every non-security-focused test harness
// used before fase 8 — extracted here, mechanically, so it exists in one named place instead of
// eight literal copies. These harnesses exercise business logic (resharding, redirects, retention,
// interceptor pass-through) unrelated to TLS itself; each pairs this with an explicit
// AllowInsecureTransport = true on its own RaftServerOptions/RemontoireClientOptions, the same
// honest, logged dev/test escape hatch fase 8 introduced (§6.1) — never a silent bypass.
static class InsecureKestrelTestHost {
    public static void ConfigureLoopbackHttp2(this KestrelServerOptions options) =>
        options.Listen(IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
}
