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
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown MetaLogRecordType tag."),
        };
    }

    static RegisterGroup ReadRegisterGroup(BinaryReader reader) {
        var groupId = reader.ReadString();
        var memberCount = reader.ReadInt32();
        var members = new ShardGroupMember[memberCount];
        for (var i = 0; i < memberCount; i++)
            members[i] = new ShardGroupMember(reader.ReadString(), new Uri(reader.ReadString()));
        return new RegisterGroup(groupId, members);
    }
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
