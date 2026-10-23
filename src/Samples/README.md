# Remontoire samples

Samples that host Remontoire from its NuGet packages.

## Packages first

From the repository root:

```bash
dotnet build src/Remontoire.slnx -c Release
dotnet pack src/Remontoire.slnx -c Release --no-build -o src/Samples/LocalPackages
```

The packages are generated on build, so `dotnet pack` packs the existing Release output and doesn't
build it itself.

After packing a new build of the same version, delete the Remontoire packages from the local NuGet
cache (`~/.nuget/packages/remontoire*`) first — otherwise a restore keeps using the old one.

## ClusterSample

Three nodes, each hosting the data group `group-1` and a meta-group replica. The client port is
6000 and the peer port is 6001 inside the container; the client ports are published as 6001–6003 on
the host. Start it with `docker compose up --build` from `src/Samples/ClusterSample`, stop with
`docker compose down` (`--volumes` also wipes the data). A leader announces itself in the logs as
`Became leader of group-1` and `Became leader of __meta__`. Redirects and the group members use the
host names of the compose network (`node-1`…), so a client should run inside that network too.
Health lives on port 8080, plain HTTP/1.1 (`/health/live` and `/health/ready`), reachable inside
the compose network: the client and peer ports speak HTTP/2 only, which an HTTP/1.1 probe cannot
talk to.

Nodes take no declarations: every service that produces or consumes declares what it owns itself,
as a client — `AddRemontoire().AddClient(...).AddStream("orders")` for the stream a service
publishes to, `.AddConsumerGroup("orders", "billing")` for the group it consumes with. The client
applies the declarations through the admin endpoint on the client port when its host starts. Each
node registers the data group it hosts in the meta log under its `ClientAddress`, which is how
clients and new streams find the cluster's groups.

## TLS on the peer port

Only the peer port uses TLS with a mandatory client certificate; the client port stays plain, and
the host secures it as it sees fit. Generate the certificates in `src/Samples/ClusterSample/certs`
with the recipe below, then start with
`docker compose -f docker-compose.yml -f docker-compose.tls.yml up --build`.

```bash
mkdir certs && cd certs
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 365 -subj "/CN=remontoire-sample-ca"
for node in node-1 node-2 node-3; do
  openssl req -newkey rsa:2048 -nodes -keyout $node.key -out $node.csr -subj "/CN=$node"
  printf "subjectAltName=DNS:$node\nextendedKeyUsage=serverAuth,clientAuth\n" > $node.ext
  openssl x509 -req -in $node.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out $node.crt -days 365 -extfile $node.ext
  openssl pkcs12 -export -out $node.pfx -inkey $node.key -in $node.crt -passout pass:
done
```

## EmbeddedSample

A web app with Remontoire embedded: `POST /greetings?name=Ada` publishes a greeting, a background
consumer handles and acks each one, and `GET /greetings` lists the greetings handled since the
app started. A restart doesn't hand out acked greetings again. A greeting that fails to be handled
is nacked: it comes back after the group's backoff, before any later greeting, and is parked
after the last retry. Start it with `dotnet run` from
`src/Samples/EmbeddedSample`; the data lives in `data/` (setting `Remontoire:DataDirectory`).
`EmbeddedSample.Tests` hosts the app in a `WebApplicationFactory`:
`dotnet test src/Samples/EmbeddedSample.Tests`.

The app itself owns its observability: `GET /health/live` and `GET /health/ready` report its
health checks, `GET /metrics` serves Prometheus metrics, and spans go out over OTLP once
`OTEL_EXPORTER_OTLP_ENDPOINT` is set. It traces its requests too, so each publish span is the child
of the request that published it. The sample test checks the readiness endpoint, the published
metrics and the exported publish/receive spans.
