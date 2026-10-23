namespace Remontoire.Sharding;

/// <summary>
/// One consumer group's policy for one stream: how its acks replicate, whether it holds back
/// retention, and how often and how fast a nacked message is offered again. Always resolved through
/// <see cref="ShardAssignmentTable.GetConsumerGroupPolicy"/>'s well-defined default (the defaults
/// of <see cref="ConsumerGroupOptions"/>) rather than a <c>TryGetXxx</c>/exception shape — unlike
/// <see cref="StreamShardingConfig"/>, where an unknown stream is a real error (the stream was
/// never created), an unknown consumer-group policy means the group was never declared.
/// </summary>
/// <param name="Mode">See <see cref="ConsumerGroupOptions.AckMode"/>.</param>
/// <param name="Mandatory">See <see cref="ConsumerGroupOptions.Mandatory"/>.</param>
/// <param name="MaxRetries">See <see cref="ConsumerGroupOptions.MaxRetries"/>.</param>
/// <param name="InitialRetryDelay">See <see cref="ConsumerGroupOptions.InitialRetryDelay"/>.</param>
/// <param name="MaxRetryDelay">See <see cref="ConsumerGroupOptions.MaxRetryDelay"/>.</param>
public readonly record struct ConsumerGroupPolicy(AckMode Mode, bool Mandatory, int MaxRetries, TimeSpan InitialRetryDelay, TimeSpan MaxRetryDelay) {
    /// <summary>
    /// The policy <paramref name="options"/> declare — the one translation from options to policy,
    /// for a declaration and for the default of an undeclared group alike.
    /// </summary>
    /// <param name="options">A consumer group's options.</param>
    /// <returns>The policy.</returns>
    public static ConsumerGroupPolicy From(ConsumerGroupOptions options) =>
        new(options.AckMode, options.Mandatory, options.MaxRetries, options.InitialRetryDelay, options.MaxRetryDelay);
}
