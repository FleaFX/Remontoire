using System.Collections.Concurrent;

namespace Remontoire.Sharding;

public sealed partial class ShardAssignmentTable {
    readonly ConcurrentDictionary<(string StreamName, string ConsumerGroup), ConsumerGroupPolicy> _consumerGroupPolicies = new();
    readonly ConcurrentDictionary<string, StreamRetentionPolicy> _retentionPolicies = new();

    static readonly ConsumerGroupPolicy DefaultConsumerGroupPolicy = ConsumerGroupPolicy.From(new ConsumerGroupOptions());
    static readonly StreamRetentionPolicy DefaultRetentionPolicy = new(
        AuditRetention: TimeSpan.FromDays(7), MaxRetention: TimeSpan.FromDays(30),
        MaxSizeBytesPerVirtualShard: null, CheckpointInterval: null, CheckpointOffsetCount: null);

    /// <summary>
    /// A consumer group's ack policy for a stream — <see cref="DefaultConsumerGroupPolicy"/>
    /// (the defaults of <see cref="ConsumerGroupOptions"/>) for a group that was never declared.
    /// </summary>
    public ConsumerGroupPolicy GetConsumerGroupPolicy(string streamName, string consumerGroup) =>
        _consumerGroupPolicies.GetValueOrDefault((streamName, consumerGroup), DefaultConsumerGroupPolicy);

    /// <summary>
    /// Looks up a declared consumer group's ack policy for a stream. A group is declared once any
    /// policy record names it.
    /// </summary>
    public bool TryGetConsumerGroupPolicy(string streamName, string consumerGroup, out ConsumerGroupPolicy policy) =>
        _consumerGroupPolicies.TryGetValue((streamName, consumerGroup), out policy);

    /// <summary>
    /// Every consumer group declared on <paramref name="streamName"/>, snapshotted.
    /// </summary>
    public IReadOnlyCollection<string> DeclaredConsumerGroups(string streamName) =>
        _consumerGroupPolicies.Keys.Where(key => key.StreamName == streamName).Select(key => key.ConsumerGroup).ToArray();

    /// <summary>
    /// Describes <paramref name="streamName"/>: its sharding, its policy and its declared consumer
    /// groups in ordinal name order; <see langword="null"/> for an unknown stream.
    /// </summary>
    public StreamDescription? DescribeStream(string streamName) {
        if (!_streams.TryGetValue(streamName, out var config))
            return null;

        var retention = GetRetentionPolicy(streamName);
        var consumerGroups = _consumerGroupPolicies
            .Where(pair => pair.Key.StreamName == streamName)
            .OrderBy(pair => pair.Key.ConsumerGroup, StringComparer.Ordinal)
            .Select(pair => new ConsumerGroupDescription(pair.Key.ConsumerGroup, pair.Value.Mode, pair.Value.Mandatory,
                pair.Value.MaxRetries, pair.Value.InitialRetryDelay, pair.Value.MaxRetryDelay))
            .ToArray();
        return new StreamDescription(streamName, config.VirtualShardCount, retention.MaxRetention, retention.MaxSizeBytesPerVirtualShard,
            retention.CheckpointInterval, retention.CheckpointOffsetCount, consumerGroups);
    }

    /// <summary>
    /// Describes every known stream, in ordinal name order.
    /// </summary>
    public IReadOnlyList<StreamDescription> DescribeStreams() =>
        _streams.Keys.Order(StringComparer.Ordinal).Select(streamName => DescribeStream(streamName)!).ToArray();

    /// <summary>
    /// A stream's retention/checkpoint policy — <see cref="DefaultRetentionPolicy"/> (7/30 days,
    /// no size ceiling, no checkpoint override) when no operator has ever set one.
    /// </summary>
    public StreamRetentionPolicy GetRetentionPolicy(string streamName) =>
        _retentionPolicies.GetValueOrDefault(streamName, DefaultRetentionPolicy);

    // Called from Apply(MetaLogRecord)'s existing switch (ShardAssignmentTable.cs) — that
    // method's own signature and every one of its callers stay completely unchanged; only its
    // switch body dispatches here for the five policy-record cases.
    void ApplyPolicyRecord(MetaLogRecord record) {
        switch (record) {
            case SetConsumerGroupAckMode r:
                _consumerGroupPolicies[(r.StreamName, r.ConsumerGroup)] =
                    GetConsumerGroupPolicy(r.StreamName, r.ConsumerGroup) with { Mode = r.Mode };
                break;

            case SetConsumerGroupMandatory r:
                _consumerGroupPolicies[(r.StreamName, r.ConsumerGroup)] =
                    GetConsumerGroupPolicy(r.StreamName, r.ConsumerGroup) with { Mandatory = r.Mandatory };
                break;

            case DeclareConsumerGroup r:
                _consumerGroupPolicies[(r.StreamName, r.ConsumerGroup)] =
                    new ConsumerGroupPolicy(r.Mode, r.Mandatory, r.MaxRetries, r.InitialRetryDelay, r.MaxRetryDelay);
                break;

            case SetStreamRetentionPolicy r:
                // Clamped, not trusted verbatim: a negative duration would make RetentionEvaluator's
                // own cutoff resolve into the future (dead-lettering/pruning everything immediately);
                // a negative size ceiling would make the size-based check always true (pruning every
                // segment). Neither is a value an operator (or a corrupted replayed record) should be
                // able to produce — clamp to the safe "expire immediately"/"no ceiling" equivalents.
                _retentionPolicies[r.StreamName] = GetRetentionPolicy(r.StreamName) with {
                    AuditRetention = r.AuditRetention < TimeSpan.Zero ? TimeSpan.Zero : r.AuditRetention,
                    MaxRetention = r.MaxRetention < TimeSpan.Zero ? TimeSpan.Zero : r.MaxRetention,
                    MaxSizeBytesPerVirtualShard = r.MaxSizeBytesPerVirtualShard is < 0 ? null : r.MaxSizeBytesPerVirtualShard,
                };
                break;

            case SetStreamCheckpointInterval r:
                _retentionPolicies[r.StreamName] = GetRetentionPolicy(r.StreamName) with {
                    CheckpointInterval = r.Interval, CheckpointOffsetCount = r.OffsetCount,
                };
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(record), record, "Unknown policy MetaLogRecord case.");
        }
    }
}
