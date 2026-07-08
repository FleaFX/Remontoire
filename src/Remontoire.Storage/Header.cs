namespace Remontoire.Storage;

/// <summary>
/// A single, free-form key/value metadata pair attached to a message.
/// </summary>
/// <param name="Key">The raw, UTF-8-encoded header name.</param>
/// <param name="Value">The raw header value; the store never interprets its contents.</param>
public readonly record struct Header(ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value);
