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
/// <param name="expectedSubjects">
/// The subject(s) a candidate certificate must carry, beyond being chain-valid against
/// <paramref name="caCertificate"/> — <see langword="null"/> means no additional subject check.
/// Client-side, dialing a specific peer, this is exactly that one peer's own expected subject
/// (a single-element collection). Server-side, an inbound connection's identity isn't known before
/// validation, so this is the union of every configured peer's expected subject across every
/// hosted group — accepting a candidate whose subject matches ANY of them, not one specific one.
/// A compromised-but-CA-issued certificate for the wrong node would otherwise go undetected: the
/// chain-build alone would succeed, since the certificate is technically legitimately issued, just
/// not by/for who it claims to be.
/// </param>
public sealed class PeerCertificateValidator(X509Certificate2 caCertificate, IReadOnlyCollection<string>? expectedSubjects) {
    public bool Validate(X509Certificate2 candidate, X509Chain chain, SslPolicyErrors errors) {
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Clear();
        chain.ChainPolicy.CustomTrustStore.Add(caCertificate);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

        if (!chain.Build(candidate))
            return false;

        return expectedSubjects is null || expectedSubjects.Contains(candidate.Subject);
    }
}
