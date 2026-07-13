using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Remontoire.Security;

/// <summary>
/// The one place peer-certificate trust is decided — reused both client-side
/// (<see cref="SslClientAuthenticationOptions.RemoteCertificateValidationCallback"/>) and
/// server-side (Kestrel's own <c>HttpsConnectionAdapterOptions.ClientCertificateValidation</c>),
/// so a test can validate against this exact production logic instead of a reimplementation.
/// Deliberately builds its own chain against a custom trust root rather than relying on the
/// platform's default trust store: this cluster's CA is never meant to be an OS-trusted root.
/// </summary>
public sealed class PeerCertificateValidator(X509Certificate2 caCertificate, string? expectedSubject) {
    public bool Validate(X509Certificate2 candidate, X509Chain chain, SslPolicyErrors errors) {
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Clear();
        chain.ChainPolicy.CustomTrustStore.Add(caCertificate);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

        if (!chain.Build(candidate))
            return false;

        return expectedSubject is null || candidate.Subject == expectedSubject;
    }
}
