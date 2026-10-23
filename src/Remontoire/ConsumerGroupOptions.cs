namespace Remontoire;

/// <summary>
/// The configuration a consumer group is declared with, through
/// <see cref="IRemontoireAdmin.EnsureConsumerGroupAsync"/> or <see cref="RemontoireBuilder.AddConsumerGroup"/>.
/// Every property can change later: declaring the group again with other values applies them.
/// </summary>
public sealed class ConsumerGroupOptions {
    /// <summary>
    /// How the group's acks replicate. Defaults to <see cref="AckMode.Strict"/>.
    /// </summary>
    public AckMode AckMode { get; set; } = AckMode.Strict;

    /// <summary>
    /// Whether the group holds back retention: a message is pruned only once every mandatory group
    /// acked it, or once it is older than <see cref="StreamOptions.MaxRetention"/>. Defaults to
    /// <see langword="true"/>.
    /// </summary>
    public bool Mandatory { get; set; } = true;

    /// <summary>
    /// How often a nacked message is offered again before it is parked. Zero parks a message at its
    /// first nack. Defaults to 5.
    /// </summary>
    public int MaxRetries { get; set; } = 5;

    /// <summary>
    /// The delay before the first retry of a nacked message; every next retry waits twice as long,
    /// up to <see cref="MaxRetryDelay"/>. A delay passed to <see cref="IRemontoireConsumer.NackAsync"/>
    /// takes precedence. Defaults to one second.
    /// </summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The longest delay between two retries of a nacked message. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);
}
