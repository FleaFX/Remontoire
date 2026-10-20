using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Remontoire.Tests;

/// <summary>
/// Generates a throwaway CA and CA-signed leaf certificates entirely in-process, using pure BCL
/// <see cref="CertificateRequest"/> — no NuGet package, no external tool. Test-only: it plays the
/// host's PKI in the TLS tests. This CA's private key exists only in memory for as long as the
/// caller holds it, with no revocation, rotation, or audit trail whatsoever — never use outside tests.
/// </summary>
static class DevClusterCertificateFactory {
    static readonly TimeSpan NotBeforeSkew = TimeSpan.FromMinutes(5); // tolerate minor clock skew between this process and whoever validates the cert
    static readonly TimeSpan Lifetime = TimeSpan.FromDays(7);

    /// <summary>
    /// Creates a self-signed CA certificate. Returns both the public-only certificate (the trust
    /// root passed to <see cref="IsSignedBy"/>) and the same certificate with its private key
    /// attached (needed to sign a leaf via <see cref="CreateLeaf"/>).
    /// </summary>
    public static (X509Certificate2 Public, X509Certificate2 WithPrivateKey) CreateCa(string subjectName = "CN=Remontoire Dev Cluster CA") {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(subjectName, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: true, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, critical: true));

        var notBefore = DateTimeOffset.UtcNow - NotBeforeSkew;
        using var selfSigned = request.CreateSelfSigned(notBefore, notBefore + Lifetime);

        // A certificate straight from CreateSelfSigned isn't reliably usable by SslStream/Kestrel
        // — re-exporting/reimporting via X509CertificateLoader (not the obsolete X509Certificate2
        // constructor, SYSLIB0057) is the reliable form.
        var withPrivateKey = X509CertificateLoader.LoadPkcs12(selfSigned.Export(X509ContentType.Pkcs12), password: null);
        var publicOnly = X509CertificateLoader.LoadCertificate(selfSigned.Export(X509ContentType.Cert));
        return (publicOnly, withPrivateKey);
    }

    /// <summary>
    /// Creates a leaf certificate signed by <paramref name="caWithPrivateKey"/> (as returned by
    /// <see cref="CreateCa"/>), valid for both server and client TLS authentication — a cluster
    /// node presents the same certificate in either role.
    /// </summary>
    public static X509Certificate2 CreateLeaf(X509Certificate2 caWithPrivateKey, string subjectName) {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(subjectName, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: false, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false)); // serverAuth, clientAuth

        var notBefore = DateTimeOffset.UtcNow - NotBeforeSkew;
        var notAfter = notBefore + Lifetime;

        // The issuer's own notAfter is a hard ceiling (CertificateRequest.Create throws otherwise)
        // — computed independently, a moment apart from the CA's own notBefore/notAfter above,
        // this leaf's notAfter can otherwise land a few milliseconds past the CA's, purely from
        // that timing jitter.
        var caNotAfter = new DateTimeOffset(caWithPrivateKey.NotAfter.ToUniversalTime(), TimeSpan.Zero);
        if (notAfter > caNotAfter)
            notAfter = caNotAfter;

        var serialNumber = RandomNumberGenerator.GetBytes(16);
        using var signed = request.Create(caWithPrivateKey, notBefore, notAfter, serialNumber);
        using var withPrivateKey = signed.CopyWithPrivateKey(key);

        // Same re-export/reimport reliability concern as CreateCa above.
        return X509CertificateLoader.LoadPkcs12(withPrivateKey.Export(X509ContentType.Pkcs12), password: null);
    }

    /// <summary>
    /// Whether <paramref name="candidate"/> chains to <paramref name="ca"/>, built against that one
    /// custom root instead of the platform's trust store — the host-side trust decision the TLS
    /// tests plug into Kestrel and into the transport's handler. Ignores the name check: test
    /// certificates are issued for node names, not for the loopback address they are dialed on.
    /// </summary>
    public static bool IsSignedBy(X509Certificate2 ca, X509Certificate2 candidate, X509Chain chain) {
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Clear();
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(candidate);
    }
}
