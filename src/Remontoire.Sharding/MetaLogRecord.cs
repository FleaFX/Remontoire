using System.Text;

namespace Remontoire.Sharding;

/// <summary>
/// One committed admin command in the meta-group's own log — a create-stream, register-group,
/// or resharding-lifecycle command. Encoded as its own small binary format rather than protobuf:
/// admin-command volume is tiny compared to message volume, and this project carries no project
/// references it would otherwise need in order to reuse an existing wire format.
/// </summary>
public abstract record MetaLogRecord {
    /// <summary>
    /// Encodes <paramref name="record"/> into its on-the-wire binary form, suitable for a
    /// <c>WalRecord</c>/<c>AppendRequest</c> payload.
    /// </summary>
    public static byte[] Encode(MetaLogRecord record) {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
            WriteBody(record, writer);
        return stream.ToArray();
    }

    /// <summary>
    /// Decodes a <see cref="MetaLogRecord"/> previously produced by <see cref="Encode"/>.
    /// </summary>
    public static MetaLogRecord Decode(ReadOnlySpan<byte> source) {
        using var stream = new MemoryStream(source.ToArray());
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
        return ReadBody(reader);
    }

    static void WriteBody(MetaLogRecord record, BinaryWriter writer) {
        switch (record) {
            case CreateStream r:
                writer.Write((byte)MetaLogRecordType.CreateStream);
                writer.Write(r.StreamName);
                writer.Write(r.VirtualShardCount);
                writer.Write((byte)r.RoutingAlgorithm);
                break;

            case RegisterGroup r:
                writer.Write((byte)MetaLogRecordType.RegisterGroup);
                writer.Write(r.GroupId);
                writer.Write(r.Members.Count);
                foreach (var member in r.Members) {
                    writer.Write(member.NodeId);
                    writer.Write(member.Address.ToString());
                }
                break;

            case MigrationStarted r:
                writer.Write((byte)MetaLogRecordType.MigrationStarted);
                writer.Write(r.MigrationId.Value.ToString());
                writer.Write(r.StreamName);
                writer.Write(r.VirtualShardIndex);
                writer.Write(r.FromGroupId);
                writer.Write(r.ToGroupId);
                break;

            case MigrationAborted r:
                writer.Write((byte)MetaLogRecordType.MigrationAborted);
                writer.Write(r.MigrationId.Value.ToString());
                writer.Write(r.StreamName);
                writer.Write(r.VirtualShardIndex);
                break;

            case Cutover r:
                writer.Write((byte)MetaLogRecordType.Cutover);
                writer.Write(r.MigrationId.Value.ToString());
                writer.Write(r.StreamName);
                writer.Write(r.VirtualShardIndex);
                writer.Write(r.ToGroupId);
                break;

            case MigrationCompleted r:
                writer.Write((byte)MetaLogRecordType.MigrationCompleted);
                writer.Write(r.MigrationId.Value.ToString());
                writer.Write(r.StreamName);
                writer.Write(r.VirtualShardIndex);
                break;

            case SetConsumerGroupAckMode r:
                writer.Write((byte)MetaLogRecordType.SetConsumerGroupAckMode);
                writer.Write(r.StreamName);
                writer.Write(r.ConsumerGroup);
                writer.Write((byte)r.Mode);
                break;

            case SetConsumerGroupMandatory r:
                writer.Write((byte)MetaLogRecordType.SetConsumerGroupMandatory);
                writer.Write(r.StreamName);
                writer.Write(r.ConsumerGroup);
                writer.Write(r.Mandatory);
                break;

            case SetStreamRetentionPolicy r:
                writer.Write((byte)MetaLogRecordType.SetStreamRetentionPolicy);
                writer.Write(r.StreamName);
                writer.Write(r.AuditRetention.Ticks);
                writer.Write(r.MaxRetention.Ticks);
                WriteNullableInt64(writer, r.MaxSizeBytesPerVirtualShard);
                break;

            case SetStreamCheckpointInterval r:
                writer.Write((byte)MetaLogRecordType.SetStreamCheckpointInterval);
                writer.Write(r.StreamName);
                WriteNullableInt64(writer, r.Interval?.Ticks);
                WriteNullableInt32(writer, r.OffsetCount);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(record), record, "Unknown MetaLogRecord case.");
        }
    }

    static MetaLogRecord ReadBody(BinaryReader reader) {
        var type = (MetaLogRecordType)reader.ReadByte();
        return type switch {
            MetaLogRecordType.CreateStream =>
                new CreateStream(reader.ReadString(), reader.ReadInt32(), (RoutingAlgorithm)reader.ReadByte()),
            MetaLogRecordType.RegisterGroup => ReadRegisterGroup(reader),
            MetaLogRecordType.MigrationStarted =>
                new MigrationStarted(new MigrationId(Guid.Parse(reader.ReadString())), reader.ReadString(), reader.ReadInt32(), reader.ReadString(), reader.ReadString()),
            MetaLogRecordType.MigrationAborted =>
                new MigrationAborted(new MigrationId(Guid.Parse(reader.ReadString())), reader.ReadString(), reader.ReadInt32()),
            MetaLogRecordType.Cutover =>
                new Cutover(new MigrationId(Guid.Parse(reader.ReadString())), reader.ReadString(), reader.ReadInt32(), reader.ReadString()),
            MetaLogRecordType.MigrationCompleted =>
                new MigrationCompleted(new MigrationId(Guid.Parse(reader.ReadString())), reader.ReadString(), reader.ReadInt32()),
            MetaLogRecordType.SetConsumerGroupAckMode =>
                new SetConsumerGroupAckMode(reader.ReadString(), reader.ReadString(), ReadAckMode(reader)),
            MetaLogRecordType.SetConsumerGroupMandatory =>
                new SetConsumerGroupMandatory(reader.ReadString(), reader.ReadString(), reader.ReadBoolean()),
            MetaLogRecordType.SetStreamRetentionPolicy =>
                new SetStreamRetentionPolicy(reader.ReadString(), TimeSpan.FromTicks(reader.ReadInt64()), TimeSpan.FromTicks(reader.ReadInt64()), ReadNullableInt64(reader)),
            MetaLogRecordType.SetStreamCheckpointInterval =>
                ReadSetStreamCheckpointInterval(reader),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown MetaLogRecordType tag."),
        };
    }

    // A corrupted record, or a byte written by a future version with a third mode, must not
    // silently decode into an undefined AckMode — downstream, neither Mode == Strict nor
    // Mode == Checkpoint would match, an inconsistent combination neither mode intends.
    static AckMode ReadAckMode(BinaryReader reader) {
        var mode = (AckMode)reader.ReadByte();
        return Enum.IsDefined(mode) ? mode : throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown AckMode value.");
    }

    static RegisterGroup ReadRegisterGroup(BinaryReader reader) {
        var groupId = reader.ReadString();
        var memberCount = reader.ReadInt32();
        var members = new ShardGroupMember[memberCount];
        for (var i = 0; i < memberCount; i++)
            members[i] = new ShardGroupMember(reader.ReadString(), new Uri(reader.ReadString()));
        return new RegisterGroup(groupId, members);
    }

    static SetStreamCheckpointInterval ReadSetStreamCheckpointInterval(BinaryReader reader) {
        var streamName = reader.ReadString();
        var intervalTicks = ReadNullableInt64(reader);
        var offsetCount = ReadNullableInt32(reader);
        return new SetStreamCheckpointInterval(streamName, intervalTicks is { } ticks ? TimeSpan.FromTicks(ticks) : null, offsetCount);
    }

    static void WriteNullableInt64(BinaryWriter writer, long? value) {
        writer.Write(value.HasValue);
        if (value.HasValue)
            writer.Write(value.Value);
    }

    static void WriteNullableInt32(BinaryWriter writer, int? value) {
        writer.Write(value.HasValue);
        if (value.HasValue)
            writer.Write(value.Value);
    }

    static long? ReadNullableInt64(BinaryReader reader) => reader.ReadBoolean() ? reader.ReadInt64() : null;

    static int? ReadNullableInt32(BinaryReader reader) => reader.ReadBoolean() ? reader.ReadInt32() : null;
}

/// <summary>
/// Declares a stream's fixed sharding choices for the first time.
/// </summary>
public sealed record CreateStream(string StreamName, int VirtualShardCount, RoutingAlgorithm RoutingAlgorithm) : MetaLogRecord;

/// <summary>
/// Declares a physical group's identity and current membership.
/// </summary>
public sealed record RegisterGroup(string GroupId, IReadOnlyList<ShardGroupMember> Members) : MetaLogRecord;

/// <summary>
/// Begins migrating one virtual shard from <see cref="FromGroupId"/> to <see cref="ToGroupId"/>.
/// Routing is unaffected until a matching <see cref="Cutover"/> commits.
/// </summary>
public sealed record MigrationStarted(MigrationId MigrationId, string StreamName, int VirtualShardIndex, string FromGroupId, string ToGroupId) : MetaLogRecord;

/// <summary>
/// Abandons an in-progress migration; routing was never affected by it.
/// </summary>
public sealed record MigrationAborted(MigrationId MigrationId, string StreamName, int VirtualShardIndex) : MetaLogRecord;

/// <summary>
/// The single, atomic routing flip for an in-progress migration. <see cref="MigrationId"/> is
/// an idempotency token, guarding against a stale or duplicate cutover being applied twice.
/// </summary>
public sealed record Cutover(MigrationId MigrationId, string StreamName, int VirtualShardIndex, string ToGroupId) : MetaLogRecord;

/// <summary>
/// Marks a migration's post-cutover cleanup as done, after its grace period elapsed.
/// </summary>
public sealed record MigrationCompleted(MigrationId MigrationId, string StreamName, int VirtualShardIndex) : MetaLogRecord;

/// <summary>
/// Sets one consumer group's ack-replication mode for one stream — strict (every ack is its own
/// Raft-committed record) or checkpoint (acks apply locally on the leader; only a periodic
/// low-watermark is replicated). A separate command from <see cref="SetConsumerGroupMandatory"/>
/// rather than one combined command: the two have different authorization actors (ack-mode is an
/// application-owner choice; mandatory/best-effort is reserved to a cluster-wide operator) — a
/// future authorization interceptor must be able to gate one without the other.
/// </summary>
public sealed record SetConsumerGroupAckMode(string StreamName, string ConsumerGroup, AckMode Mode) : MetaLogRecord;

/// <summary>
/// Marks one consumer group as mandatory (blocks pruning until it acks, the default) or
/// best-effort (never blocks pruning) for one stream. Committed ungated for now — no
/// authorization mechanism exists yet; this command's own, separate <see cref="MetaLogRecordType"/>
/// tag is precisely what lets a later interceptor gate it independently of
/// <see cref="SetConsumerGroupAckMode"/> without touching this wire format again.
/// </summary>
public sealed record SetConsumerGroupMandatory(string StreamName, string ConsumerGroup, bool Mandatory) : MetaLogRecord;

/// <summary>
/// Sets one stream's audit-retention window (kept this long after every mandatory group has
/// acked, purely for troubleshooting), hard max-retention window (an absolute ceiling from
/// ingest, regardless of ack status), and per-virtual-shard emergency size ceiling
/// (<see langword="null"/>: no size-based emergency pruning for this stream).
/// </summary>
public sealed record SetStreamRetentionPolicy(string StreamName, TimeSpan AuditRetention, TimeSpan MaxRetention, long? MaxSizeBytesPerVirtualShard) : MetaLogRecord;

/// <summary>
/// Sets one stream's checkpoint-replication cadence for every consumer group on it using
/// <see cref="AckMode.Checkpoint"/> — whichever of <paramref name="Interval"/>/<paramref name="OffsetCount"/>
/// is reached first triggers the next checkpoint. Either may be <see langword="null"/> (that
/// trigger disabled); both <see langword="null"/> falls back to <see cref="StreamRetentionPolicy"/>'s
/// own default.
/// </summary>
public sealed record SetStreamCheckpointInterval(string StreamName, TimeSpan? Interval, int? OffsetCount) : MetaLogRecord;
