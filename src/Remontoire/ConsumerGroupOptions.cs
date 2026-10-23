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
}
