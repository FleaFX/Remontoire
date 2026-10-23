namespace Remontoire;

/// <summary>
/// A stream or consumer group declared on the <see cref="RemontoireBuilder"/>, ensured when the host
/// starts. Registered as a singleton per declaration, in the order they were added.
/// </summary>
public abstract record RemontoireDeclaration;

/// <summary>
/// A stream declared with <see cref="RemontoireBuilder.AddStream"/>.
/// </summary>
/// <param name="StreamName">The stream.</param>
/// <param name="Options">Its configuration.</param>
public sealed record StreamDeclaration(string StreamName, StreamOptions Options) : RemontoireDeclaration;

/// <summary>
/// A consumer group declared with <see cref="RemontoireBuilder.AddConsumerGroup"/>.
/// </summary>
/// <param name="StreamName">The stream the group consumes.</param>
/// <param name="ConsumerGroup">The consumer group.</param>
/// <param name="Options">Its configuration.</param>
public sealed record ConsumerGroupDeclaration(string StreamName, string ConsumerGroup, ConsumerGroupOptions Options) : RemontoireDeclaration;
