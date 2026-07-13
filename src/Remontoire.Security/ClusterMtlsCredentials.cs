using System.Security.Cryptography.X509Certificates;

namespace Remontoire.Security;

/// <summary>
/// The resolved certificates a node needs for cluster mTLS: the CA's own public certificate (the
/// trust root peers are validated against) and this node's own certificate, signed by that CA.
/// </summary>
public sealed record ClusterMtlsCredentials(X509Certificate2 CaCertificate, X509Certificate2 NodeCertificate);

/// <summary>
/// Loads <see cref="ClusterMtlsCredentials"/> from the file paths in a <see cref="ClusterMtlsOptions"/>,
/// once at startup.
/// </summary>
public static class ClusterMtlsCredentialsLoader {
    public static ClusterMtlsCredentials Load(ClusterMtlsOptions options) {
        var ca = X509CertificateLoader.LoadCertificateFromFile(options.ClusterCaCertificatePath);
        try {
            var node = string.IsNullOrEmpty(options.NodeCertificatePassword)
                ? X509Certificate2.CreateFromPemFile(options.NodeCertificatePath, options.NodeCertificateKeyPath)
                : X509Certificate2.CreateFromEncryptedPemFile(options.NodeCertificatePath, options.NodeCertificatePassword, options.NodeCertificateKeyPath);
            return new ClusterMtlsCredentials(ca, node);
        } catch {
            ca.Dispose();
            throw;
        }
    }
}
