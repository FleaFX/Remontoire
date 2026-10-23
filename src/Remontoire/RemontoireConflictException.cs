namespace Remontoire;

/// <summary>
/// Thrown when a declaration asks for a configuration an existing stream cannot change to.
/// </summary>
/// <param name="message">What exists and what the declaration asks for.</param>
public sealed class RemontoireConflictException(string message) : InvalidOperationException(message);
