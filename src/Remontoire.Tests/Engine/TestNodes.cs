using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Remontoire.Raft.Grpc;
using Remontoire.Sharding;

namespace Remontoire.Engine;

// Shared composition helpers for the tests: Metrics(...) builds the RemontoireMetrics a real host
// registers over the same registries — with its own IMeterFactory, so parallel test classes never
// share a meter — and Node(...) fills in a fresh instance of whatever a test leaves null. A test
// that wants to read a node's metrics creates them first with Metrics(...) over the same registries
// and hands them in.
internal static class TestNodes {
    internal static RemontoireMetrics Metrics(RaftReplicaRegistry raftRegistry, MessagingGroupRegistry messagingRegistry, ShardAssignmentTable assignmentTable) {
        var meterFactory = new ServiceCollection().AddMetrics().BuildServiceProvider().GetRequiredService<IMeterFactory>();
        return new RemontoireMetrics(meterFactory, raftRegistry, messagingRegistry, assignmentTable);
    }

    internal static RemontoireNode Node(
        RaftReplicaRegistry? registry = null,
        MessagingGroupRegistry? messagingRegistry = null,
        ShardAssignmentTable? assignmentTable = null,
        MetaLogJournal? metaLogJournal = null,
        MigrationAdmissionGate? admissionGate = null,
        RemontoireMetrics? metrics = null,
        ILoggerFactory? loggerFactory = null
    ) {
        registry ??= new RaftReplicaRegistry();
        messagingRegistry ??= new MessagingGroupRegistry();
        assignmentTable ??= new ShardAssignmentTable();

        return new RemontoireNode(
            registry,
            messagingRegistry,
            assignmentTable,
            metaLogJournal ?? new MetaLogJournal(),
            admissionGate ?? new MigrationAdmissionGate(),
            metrics ?? Metrics(registry, messagingRegistry, assignmentTable),
            loggerFactory ?? NullLoggerFactory.Instance
        );
    }
}
