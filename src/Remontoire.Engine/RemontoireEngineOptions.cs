namespace Remontoire.Engine;

/// <summary>
/// Configures the embedded engine registered by <see cref="RemontoireEngineBuilderExtensions.AddEmbedded"/>.
/// </summary>
public sealed class RemontoireEngineOptions {
    /// <summary>
    /// Where the engine keeps its data: the meta log in <c>meta</c>, each stream in
    /// <c>groups/&lt;stream&gt;</c>. Required; one process at a time.
    /// </summary>
    public string DataDirectory { get; set; } = "";

    /// <summary>
    /// The address remote clients reach this process at, when the host maps the endpoints of
    /// <c>Remontoire.Server</c>: a remote client finds a stream's node through it. Registered for
    /// every stream at each start, so it may change between starts. Without it, only in-process use
    /// works.
    /// </summary>
    public Uri? ClientAddress { get; set; }

    /// <summary>
    /// How often retention runs per stream; <see langword="null"/> keeps the default.
    /// </summary>
    internal TimeSpan? RetentionTickInterval { get; set; }
}
