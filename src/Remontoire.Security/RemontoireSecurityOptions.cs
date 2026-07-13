namespace Remontoire.Security;

/// <summary>
/// Binds the "Security" configuration section: how to validate an incoming bearer token, and
/// which of its claims identify the calling subject/role.
/// </summary>
public sealed class RemontoireSecurityOptions {
    /// <summary>
    /// The token issuer, used for OIDC discovery/JWKS retrieval.
    /// </summary>
    public string Authority { get; set; } = "";

    /// <summary>
    /// The expected audience of an incoming access token.
    /// </summary>
    public string Audience { get; set; } = "";

    /// <summary>
    /// Whether the authority's metadata endpoint must be served over HTTPS. Only ever <see langword="false"/>
    /// in development/test, never in a real deployment.
    /// </summary>
    public bool RequireHttpsMetadata { get; set; } = true;

    /// <summary>
    /// The claim type carrying the caller's role(s) — checked by <see cref="RemontoireAuthorizer.HasRole"/>.
    /// Not every identity provider names this claim "role" — Microsoft Entra ID's own v2.0 app-role
    /// claim, for example, is named "roles" (plural).
    /// </summary>
    public string RoleClaimType { get; set; } = "role";

    /// <summary>
    /// The role value <see cref="RemontoireAuthorizer.IsOperator"/> checks for.
    /// </summary>
    public string OperatorRoleValue { get; set; } = "operator";

    /// <summary>
    /// The claim type identifying the calling subject for ACL purposes, when a stream has no
    /// per-stream override (<see cref="Sharding.ShardAssignmentTable.GetSubjectClaimTypeOverride"/>).
    /// Defaults to "client_id" rather than "sub": Remontoire's data plane is overwhelmingly
    /// machine-to-machine (OAuth2 client-credentials), and "client_id" identifies the calling
    /// client unambiguously regardless of grant type, whereas "sub" only reliably equals the
    /// client id for a client-credentials token when the identity provider follows RFC 9068 —
    /// some omit "sub" from app-only tokens entirely.
    /// </summary>
    public string SubjectClaimType { get; set; } = "client_id";
}
