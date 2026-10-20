using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace Remontoire.Tests;

// The "loopback, HTTP/2, no TLS" Kestrel config shared by every test harness that exercises
// business logic (resharding, redirects, retention) rather than TLS — plain http:// addresses on
// both sides. TLS is host configuration; RaftGrpcMtlsClusterTests shows it.
static class InsecureKestrelTestHost {
    public static void ConfigureLoopbackHttp2(this KestrelServerOptions options) =>
        options.Listen(IPAddress.Loopback, 0, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
}
