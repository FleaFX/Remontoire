using System.Security.Claims;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Remontoire.Sharding;

namespace Remontoire.Security;

public class RemontoireAuthorizerTests {
    static RemontoireAuthorizer Authorizer(ShardAssignmentTable? table = null, RemontoireSecurityOptions? options = null) =>
        new(table ?? new ShardAssignmentTable(), Options.Create(options ?? new RemontoireSecurityOptions()));

    // ClaimsIdentity's authenticationType argument must be non-empty, or IsAuthenticated is
    // always false — a silent trap for any test building an "authenticated" principal.
    static ClaimsPrincipal Principal(params Claim[] claims) => new(new ClaimsIdentity(claims, "TestAuthType"));

    public class IsOperator {
        [Fact]
        public void Returns_true_when_the_user_carries_the_configured_operator_role_value() {
            var authorizer = Authorizer();
            var user = Principal(new Claim("role", "operator"));

            authorizer.IsOperator(user).Should().BeTrue();
        }

        [Fact]
        public void Returns_false_when_the_user_carries_a_different_role_value() {
            var authorizer = Authorizer();
            var user = Principal(new Claim("role", "auditor"));

            authorizer.IsOperator(user).Should().BeFalse();
        }

        [Fact]
        public void Returns_false_when_the_user_carries_no_role_claim_at_all() {
            var authorizer = Authorizer();
            var user = Principal();

            authorizer.IsOperator(user).Should().BeFalse();
        }
    }

    public class HasRole {
        [Fact]
        public void Is_independent_of_IsOperator() {
            var authorizer = Authorizer();
            var user = Principal(new Claim("role", "auditor"));

            authorizer.HasRole(user, "auditor").Should().BeTrue();
            authorizer.IsOperator(user).Should().BeFalse("carrying the auditor role must not also satisfy the operator check");
        }
    }

    public class CanProduce {
        [Fact]
        public void Returns_false_when_the_user_carries_no_subject_claim() {
            var authorizer = Authorizer();
            var user = Principal();

            authorizer.CanProduce(user, "orders").Should().BeFalse();
        }

        [Fact]
        public void Returns_false_when_the_subject_has_no_acl_grant() {
            var authorizer = Authorizer();
            var user = Principal(new Claim("client_id", "client-1"));

            authorizer.CanProduce(user, "orders").Should().BeFalse();
        }

        [Fact]
        public void Returns_true_when_the_subject_has_a_matching_acl_grant() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetProduceAcl("client-1", "orders", true));
            var authorizer = Authorizer(table);
            var user = Principal(new Claim("client_id", "client-1"));

            authorizer.CanProduce(user, "orders").Should().BeTrue();
        }

        [Fact]
        public void An_operator_role_does_not_substitute_for_a_missing_acl_grant() {
            var authorizer = Authorizer();
            var user = Principal(new Claim("role", "operator"), new Claim("client_id", "client-1"));

            authorizer.CanProduce(user, "orders").Should().BeFalse("the role/ACL axes stay orthogonal — an operator needs an explicit grant too");
        }
    }

    public class CanConsume {
        [Fact]
        public void Returns_true_when_the_subject_has_a_matching_acl_grant() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetConsumeAcl("client-1", "orders", "billing", true));
            var authorizer = Authorizer(table);
            var user = Principal(new Claim("client_id", "client-1"));

            authorizer.CanConsume(user, "orders", "billing").Should().BeTrue();
        }

        [Fact]
        public void Returns_false_for_a_different_consumer_group() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetConsumeAcl("client-1", "orders", "billing", true));
            var authorizer = Authorizer(table);
            var user = Principal(new Claim("client_id", "client-1"));

            authorizer.CanConsume(user, "orders", "shipping").Should().BeFalse();
        }
    }

    public class PerStreamSubjectClaimOverride {
        [Fact]
        public void A_stream_without_an_override_resolves_the_subject_via_the_clusterwide_default() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetProduceAcl("client-1", "orders", true));
            var authorizer = Authorizer(table);
            // Carries both the default claim ("client_id") and another one ("sub") with a
            // different value — a wrong resolution would silently pick the wrong claim and fail.
            var user = Principal(new Claim("client_id", "client-1"), new Claim("sub", "user-42"));

            authorizer.CanProduce(user, "orders").Should().BeTrue();
        }

        [Fact]
        public void A_stream_with_an_override_resolves_the_subject_via_the_overridden_claim_instead() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetStreamSubjectClaimType("orders", "sub"));
            table.Apply(new SetProduceAcl("user-42", "orders", true));
            var authorizer = Authorizer(table);
            var user = Principal(new Claim("client_id", "client-1"), new Claim("sub", "user-42"));

            authorizer.CanProduce(user, "orders").Should().BeTrue();
        }

        [Fact]
        public void A_stream_with_an_override_no_longer_matches_a_grant_keyed_by_the_defaults_value() {
            var table = new ShardAssignmentTable();
            table.Apply(new SetStreamSubjectClaimType("orders", "sub"));
            table.Apply(new SetProduceAcl("client-1", "orders", true)); // granted under the pre-override (default) claim's value
            var authorizer = Authorizer(table);
            var user = Principal(new Claim("client_id", "client-1"), new Claim("sub", "user-42"));

            authorizer.CanProduce(user, "orders").Should().BeFalse("the override now resolves the subject via \"sub\", not \"client_id\" — the old grant no longer applies");
        }
    }
}
