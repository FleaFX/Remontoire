namespace Remontoire.Sharding;

/// <summary>
/// One consumer group's ack policy for one stream. Always resolved through
/// <see cref="ShardAssignmentTable.GetConsumerGroupPolicy"/>'s well-defined default
/// (<see cref="AckMode.Strict"/> + mandatory) rather than a <c>TryGetXxx</c>/exception shape —
/// unlike <see cref="StreamShardingConfig"/>, where an unknown stream is a real error (the stream
/// was never created), an unknown consumer-group policy here simply means no operator has ever
/// overridden it, and "strict + mandatory" is exactly the pre-existing default behavior.
/// </summary>
public readonly record struct ConsumerGroupPolicy(AckMode Mode, bool Mandatory);
