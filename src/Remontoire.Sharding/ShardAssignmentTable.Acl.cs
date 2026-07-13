using System.Collections.Concurrent;

namespace Remontoire.Sharding;

public sealed partial class ShardAssignmentTable {
    readonly ConcurrentDictionary<(string Subject, string StreamName), bool> _produceAcl = new();
    readonly ConcurrentDictionary<(string Subject, string StreamName, string ConsumerGroup), bool> _consumeAcl = new();
    readonly ConcurrentDictionary<string, string?> _streamSubjectClaimType = new();

    /// <summary>
    /// Whether <paramref name="subject"/> may produce onto <paramref name="streamName"/> —
    /// default-deny (<see langword="false"/>) when no operator has ever granted it. The opposite
    /// default from <see cref="GetConsumerGroupPolicy"/>'s "strict + mandatory until told
    /// otherwise": a subject that has been authenticated but never explicitly ACL'd must never
    /// gain implicit access to another tenant's stream.
    /// </summary>
    public bool CanProduce(string subject, string streamName) =>
        _produceAcl.GetValueOrDefault((subject, streamName), false);

    /// <summary>
    /// Whether <paramref name="subject"/> may consume/ack as <paramref name="consumerGroup"/> on
    /// <paramref name="streamName"/> — same default-deny reasoning as <see cref="CanProduce"/>.
    /// </summary>
    public bool CanConsume(string subject, string streamName, string consumerGroup) =>
        _consumeAcl.GetValueOrDefault((subject, streamName, consumerGroup), false);

    /// <summary>
    /// The claim-type override for <paramref name="streamName"/>, if an operator has set one —
    /// <see langword="null"/> when this stream uses the cluster-wide default. Deliberately does
    /// not resolve that default itself — this project carries no reference to whichever project
    /// owns that default's configuration, and never should.
    /// </summary>
    public string? GetSubjectClaimTypeOverride(string streamName) =>
        _streamSubjectClaimType.GetValueOrDefault(streamName);

    // Called from Apply(MetaLogRecord)'s existing switch (ShardAssignmentTable.cs) — that
    // method's own signature and every one of its callers stay completely unchanged; only its
    // switch body dispatches here for the three ACL-record cases.
    void ApplyAclRecord(MetaLogRecord record) {
        switch (record) {
            case SetProduceAcl r:
                _produceAcl[(r.Subject, r.StreamName)] = r.Allowed;
                break;

            case SetConsumeAcl r:
                _consumeAcl[(r.Subject, r.StreamName, r.ConsumerGroup)] = r.Allowed;
                break;

            case SetStreamSubjectClaimType r:
                _streamSubjectClaimType[r.StreamName] = r.ClaimType;
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(record), record, "Unknown ACL MetaLogRecord case.");
        }
    }
}
