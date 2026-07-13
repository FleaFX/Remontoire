namespace Remontoire.Security;

/// <summary>
/// Binds the "Raft:Mtls" configuration section: where this node's own cluster-CA-signed
/// certificate and the CA's own trust root live on disk, and whether insecure, unencrypted
/// transport is explicitly allowed instead (dev/test only, never production).
/// </summary>
public sealed class ClusterMtlsOptions {
    /// <summary>
    /// Path to the cluster-internal CA's public certificate — the trust root every peer's own
    /// certificate is validated against.
    /// </summary>
    public string ClusterCaCertificatePath { get; set; } = "";

    /// <summary>
    /// Path to this node's own certificate, signed by the cluster CA.
    /// </summary>
    public string NodeCertificatePath { get; set; } = "";

    /// <summary>
    /// Path to this node's own certificate's private key, a separate PEM file (not a PKCS12
    /// bundle) — the shape most PKI tooling, including a cluster-internal CA, produces.
    /// </summary>
    public string NodeCertificateKeyPath { get; set; } = "";

    /// <summary>
    /// The private key file's password, if it's encrypted. <see langword="null"/> for an
    /// unencrypted key.
    /// </summary>
    public string? NodeCertificatePassword { get; set; }

    /// <summary>
    /// Explicitly allows unencrypted, unauthenticated node-to-node transport instead of mTLS —
    /// judged solely by this flag, never by an environment-name heuristic. Never use outside
    /// development/test.
    /// </summary>
    public bool AllowInsecureTransport { get; set; }
}
