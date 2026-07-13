using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;

namespace Remontoire.Security;

public class PeerCertificateValidatorTests {
    [Fact]
    public void Accepts_a_CA_signed_certificate_with_the_expected_subject() {
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var leaf = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1");
        var validator = new PeerCertificateValidator(caPublic, expectedSubject: "CN=node-1");

        using var chain = new X509Chain();
        validator.Validate(leaf, chain, SslPolicyErrors.None).Should().BeTrue();
    }

    [Fact]
    public void Accepts_any_subject_when_no_expected_subject_is_configured() {
        // The server side never knows, before validation, which specific peer is connecting — it
        // passes null and relies solely on the chain-build (§5.2/§5.4).
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var leaf = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1");
        var validator = new PeerCertificateValidator(caPublic, expectedSubject: null);

        using var chain = new X509Chain();
        validator.Validate(leaf, chain, SslPolicyErrors.None).Should().BeTrue();
    }

    [Fact]
    public void Rejects_a_CA_signed_certificate_with_the_wrong_expected_subject() {
        // A compromised-but-CA-issued certificate for the wrong node must not be trusted just
        // because the chain itself is valid.
        var (caPublic, caWithKey) = DevClusterCertificateFactory.CreateCa();
        var leaf = DevClusterCertificateFactory.CreateLeaf(caWithKey, "CN=node-1");
        var validator = new PeerCertificateValidator(caPublic, expectedSubject: "CN=node-2");

        using var chain = new X509Chain();
        validator.Validate(leaf, chain, SslPolicyErrors.None).Should().BeFalse();
    }

    [Fact]
    public void Rejects_a_certificate_signed_by_a_different_CA_regardless_of_subject() {
        var (caPublic, _) = DevClusterCertificateFactory.CreateCa();
        var (_, rogueCaWithKey) = DevClusterCertificateFactory.CreateCa("CN=Rogue CA");
        var rogueLeaf = DevClusterCertificateFactory.CreateLeaf(rogueCaWithKey, "CN=node-1");
        var validator = new PeerCertificateValidator(caPublic, expectedSubject: null);

        using var chain = new X509Chain();
        validator.Validate(rogueLeaf, chain, SslPolicyErrors.None).Should().BeFalse("the chain must not build against a CA this validator doesn't trust");
    }

    [Fact]
    public void Rejects_a_self_signed_certificate_that_was_never_issued_by_any_CA() {
        var (caPublic, _) = DevClusterCertificateFactory.CreateCa();
        var (selfSignedPublic, _) = DevClusterCertificateFactory.CreateCa("CN=node-1"); // its own, unrelated self-signed cert
        var validator = new PeerCertificateValidator(caPublic, expectedSubject: null);

        using var chain = new X509Chain();
        validator.Validate(selfSignedPublic, chain, SslPolicyErrors.None).Should().BeFalse();
    }
}
