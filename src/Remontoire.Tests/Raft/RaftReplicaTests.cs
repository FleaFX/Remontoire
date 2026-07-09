using FluentAssertions;
using Google.Protobuf;
using Remontoire.Raft.V1;
using Remontoire.Storage;
using Remontoire.Storage.Serialization;

namespace Remontoire.Raft;

public class RaftReplicaTests {
    // Long, effectively-unreachable-in-a-test timeouts: every transition in these tests is
    // driven by direct message injection — the actor as a pure state machine — never
    // by an actually-firing background timer. A real timer racing with an injected message would
    // reintroduce exactly the class of flakiness this test layer exists to avoid.
    static RaftReplicaConfig Config(string nodeId, IReadOnlyList<RaftGroupMember> peers, ulong? snapshotThresholdEntries = null) =>
        new(GroupId: "group-1", NodeId: nodeId, Peers: peers,
            HeartbeatInterval: TimeSpan.FromMinutes(10), ElectionTimeoutMin: TimeSpan.FromMinutes(10), ElectionTimeoutMax: TimeSpan.FromMinutes(11),
            SnapshotThresholdEntries: snapshotThresholdEntries ?? 10_000);

    static async Task<(RaftReplica Replica, RecordingRaftTransport Transport)> StartAsync(string nodeId = "node-1", params RaftGroupMember[] peers) {
        var transport = new RecordingRaftTransport();
        var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), transport, Config(nodeId, peers));
        await replica.StartAsync();
        return (replica, transport);
    }

    static RaftGroupMember Peer(string nodeId) => new(nodeId, new Uri($"https://{nodeId}.local"));

    static string TempStagingDirectory() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }

    // A small, honest stopgap: some fire-and-forget background sends (Task.Run inside
    // SendVoteRequest/SendAppendEntriesAsync) are not observable via DrainAsync alone, since
    // DrainAsync only guarantees the actor loop's own message processing, not a background
    // task's completion. Layer 2's SimulatedCluster (virtual time, controlled delivery) removes
    // the need for this; until then, a short bounded poll is the pragmatic tool.
    static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null) {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(2));
        while (DateTime.UtcNow < deadline) {
            if (condition())
                return true;
            await Task.Delay(5);
        }
        return condition();
    }

    public class HandleVoteRequestReceivedAsync {
        [Fact]
        public async Task Refuses_a_stale_term_with_its_own_current_term() {
            var (replica, peer) = await StartAsync("node-1", Peer("node-2"));
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> term 1, candidate
            await replica.DrainAsync();

            var reply = new TaskCompletionSource<VoteResponse>();
            replica.TryPost(new VoteRequestReceived(
                new VoteRequest { GroupId = "group-1", Term = 0, CandidateId = "node-3", LastLogIndex = 0, LastLogTerm = 0 }, reply));
            await replica.DrainAsync();

            var response = await reply.Task;
            response.VoteGranted.Should().BeFalse();
            response.Term.Should().Be(replica.CurrentTerm);
        }

        [Fact]
        public async Task Grants_the_vote_for_a_candidate_whose_log_is_at_least_as_up_to_date_and_becomes_follower_on_the_higher_term() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));

            var reply = new TaskCompletionSource<VoteResponse>();
            replica.TryPost(new VoteRequestReceived(
                new VoteRequest { GroupId = "group-1", Term = 5, CandidateId = "node-2", LastLogIndex = 0, LastLogTerm = 0 }, reply));
            await replica.DrainAsync();

            var response = await reply.Task;
            response.VoteGranted.Should().BeTrue();
            response.Term.Should().Be(5);
            replica.Role.Should().Be(ReplicaRole.Follower);
            replica.CurrentTerm.Should().Be(5);
            replica.VotedFor.Should().Be("node-2");
        }

        [Fact]
        public async Task Refuses_a_second_candidate_in_the_same_term_after_already_voting() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"), Peer("node-3"));

            var firstReply = new TaskCompletionSource<VoteResponse>();
            replica.TryPost(new VoteRequestReceived(
                new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-2", LastLogIndex = 0, LastLogTerm = 0 }, firstReply));
            await replica.DrainAsync();
            (await firstReply.Task).VoteGranted.Should().BeTrue();

            var secondReply = new TaskCompletionSource<VoteResponse>();
            replica.TryPost(new VoteRequestReceived(
                new VoteRequest { GroupId = "group-1", Term = 1, CandidateId = "node-3", LastLogIndex = 0, LastLogTerm = 0 }, secondReply));
            await replica.DrainAsync();

            (await secondReply.Task).VoteGranted.Should().BeFalse();
            replica.VotedFor.Should().Be("node-2");
        }

        [Fact]
        public async Task Refuses_a_candidate_whose_log_is_behind() {
            // Single-node group: becomes its own leader immediately, with a real NoOp entry
            // (index 1, term 1) in its log — giving it something an "empty" candidate can be behind.
            var (replica, _) = await StartAsync("node-1");
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration));
            await replica.DrainAsync();
            replica.Role.Should().Be(ReplicaRole.Leader); // sanity check on the setup

            var reply = new TaskCompletionSource<VoteResponse>();
            replica.TryPost(new VoteRequestReceived(
                new VoteRequest { GroupId = "group-1", Term = 10, CandidateId = "node-2", LastLogIndex = 0, LastLogTerm = 0 }, reply));
            await replica.DrainAsync();

            (await reply.Task).VoteGranted.Should().BeFalse("the candidate's empty log is behind our own log entries");
        }
    }

    public class HandleAppendEntriesReceivedAsync {
        [Fact]
        public async Task Refuses_a_stale_leader_term() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> term 1
            await replica.DrainAsync();

            var reply = new TaskCompletionSource<AppendEntriesResponse>();
            replica.TryPost(new AppendEntriesReceived(
                new AppendEntriesRequest { GroupId = "group-1", Term = 0, LeaderId = "node-2", PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommit = 0 }, reply));
            await replica.DrainAsync();

            var response = await reply.Task;
            response.Success.Should().BeFalse();
            response.Term.Should().Be(replica.CurrentTerm);
        }

        [Fact]
        public async Task Becomes_follower_of_a_leader_with_an_equal_or_higher_term() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));

            var reply = new TaskCompletionSource<AppendEntriesResponse>();
            replica.TryPost(new AppendEntriesReceived(
                new AppendEntriesRequest { GroupId = "group-1", Term = 3, LeaderId = "node-2", PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommit = 0 }, reply));
            await replica.DrainAsync();

            (await reply.Task).Success.Should().BeTrue();
            replica.Role.Should().Be(ReplicaRole.Follower);
            replica.CurrentTerm.Should().Be(3);
            replica.LeaderHint.Should().Be("node-2");
        }

        [Fact]
        public async Task Reports_a_conflict_when_its_log_is_shorter_than_PrevLogIndex() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));

            var reply = new TaskCompletionSource<AppendEntriesResponse>();
            replica.TryPost(new AppendEntriesReceived(
                new AppendEntriesRequest { GroupId = "group-1", Term = 1, LeaderId = "node-2", PrevLogIndex = 5, PrevLogTerm = 1, LeaderCommit = 0 }, reply));
            await replica.DrainAsync();

            var response = await reply.Task;
            response.Success.Should().BeFalse();
            response.ConflictIndex.Should().Be(1); // raftLog.LastIndex (0) + 1
            response.ConflictTerm.Should().Be(0);
        }
    }

    public class BecomeLeaderAsync {
        [Fact]
        public async Task A_single_node_group_becomes_ready_leader_immediately() {
            var (replica, _) = await StartAsync("node-1");

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration));
            await replica.DrainAsync();

            replica.Role.Should().Be(ReplicaRole.Leader);
            replica.IsLeader.Should().BeTrue();
            replica.CurrentTerm.Should().Be(1);
        }

        [Fact]
        public async Task A_multi_node_candidate_becomes_leader_on_quorum_but_is_not_ready_until_its_NoOp_commits() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"), Peer("node-3"));

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration));
            await replica.DrainAsync();
            replica.Role.Should().Be(ReplicaRole.Candidate);

            // Self-vote (1) + one granted peer vote (2) reaches quorum (2) in a 3-node group.
            replica.TryPost(new VoteResponseReceived("node-2", new VoteResponse { Term = replica.CurrentTerm, VoteGranted = true }, replica.CurrentTerm));
            await replica.DrainAsync();

            replica.Role.Should().Be(ReplicaRole.Leader);
            replica.IsLeader.Should().BeFalse("the term-opening NoOp has not been acknowledged by a quorum yet");

            replica.TryPost(new AppendEntriesResponseReceived("node-2", new AppendEntriesResponse { Term = replica.CurrentTerm, Success = true }, replica.CurrentTerm, SentUpToIndex: 1));
            await replica.DrainAsync();

            replica.IsLeader.Should().BeTrue();
        }

        [Fact]
        public async Task Recovers_nextLogicalOffset_from_persisted_state_when_the_log_tail_has_no_Append_entries() {
            var stateStore = new InMemoryRaftStateStore();
            await stateStore.SaveAsync(new RaftPersistentState(CurrentTerm: 0, VotedFor: null, SnapshotNextLogicalOffset: 100));
            var replica = new RaftReplica(stateStore, new InMemoryRaftLog(), new RecordingRaftTransport(), Config("node-1", []));
            await replica.StartAsync();

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> ready leader
            await replica.DrainAsync();

            var result = await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));

            result.LogicalOffset.Should().Be(100);
        }

        [Fact]
        public async Task Sends_a_vote_request_carrying_its_own_term_and_log_position_to_every_peer() {
            var (replica, transport) = await StartAsync("node-1", Peer("node-2"), Peer("node-3"));
            transport.OnRequestVote = (_, _) => new VoteResponse { Term = replica.CurrentTerm, VoteGranted = false };

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration));
            await replica.DrainAsync();

            (await WaitUntilAsync(() => transport.VoteRequestsSent.Count >= 2)).Should().BeTrue();
            transport.VoteRequestsSent.Should().OnlyContain(sent =>
                sent.Request.GroupId == "group-1" && sent.Request.Term == 1 && sent.Request.CandidateId == "node-1"
                && sent.Request.LastLogIndex == 0 && sent.Request.LastLogTerm == 0);
        }
    }

    public class ProposeAsync {
        [Fact]
        public async Task Throws_NotLeaderException_when_this_replica_is_not_the_ready_leader() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));

            var act = () => replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray())).AsTask();

            await act.Should().ThrowAsync<NotLeaderException>();
        }

        [Fact]
        public async Task Resolves_once_the_entry_commits_on_a_single_node_group() {
            var (replica, _) = await StartAsync("node-1");
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> ready leader
            await replica.DrainAsync();

            var result = await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));

            result.LogicalOffset.Should().Be(0);
            replica.CommitIndex.Should().BeGreaterThanOrEqualTo(result.RaftIndex);
        }

        [Fact]
        public async Task Throws_when_the_partition_key_exceeds_65535_bytes() {
            var (replica, _) = await StartAsync("node-1");
            var request = new AppendRequest(new byte[65536], [], "payload"u8.ToArray());

            var act = () => replica.ProposeAsync(request).AsTask();

            await act.Should().ThrowAsync<ArgumentException>();
        }

        [Fact]
        public async Task Throws_when_a_header_key_exceeds_65535_bytes() {
            var (replica, _) = await StartAsync("node-1");
            var request = new AppendRequest("key"u8.ToArray(), [new Header(new byte[65536], "v"u8.ToArray())], "payload"u8.ToArray());

            var act = () => replica.ProposeAsync(request).AsTask();

            await act.Should().ThrowAsync<ArgumentException>();
        }

        [Fact]
        public async Task Throws_when_there_are_more_than_65535_headers() {
            var (replica, _) = await StartAsync("node-1");
            var headers = Enumerable.Range(0, 65536).Select(_ => new Header("k"u8.ToArray(), "v"u8.ToArray())).ToArray();
            var request = new AppendRequest("key"u8.ToArray(), headers, "payload"u8.ToArray());

            var act = () => replica.ProposeAsync(request).AsTask();

            await act.Should().ThrowAsync<ArgumentException>();
        }

        [Fact]
        public async Task Does_not_post_to_the_actor_when_validation_fails() {
            var (replica, _) = await StartAsync("node-1");
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> ready leader
            await replica.DrainAsync();
            var invalid = new AppendRequest(new byte[65536], [], "payload"u8.ToArray());

            var act = () => replica.ProposeAsync(invalid).AsTask();
            await act.Should().ThrowAsync<ArgumentException>();

            var result = await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));
            result.LogicalOffset.Should().Be(0);
        }
    }

    public class TryTriggerSnapshotAsync {
        [Fact]
        public async Task Compacts_the_log_once_prepareSnapshot_resolves() {
            var raftLog = new InMemoryRaftLog();
            var config = Config("node-1", [], snapshotThresholdEntries: 0);
            var replica = new RaftReplica(new InMemoryRaftStateStore(), raftLog, new RecordingRaftTransport(), config,
                prepareSnapshot: (_, _) => Task.FromResult<IReadOnlyList<string>>([]));
            await replica.StartAsync();

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> ready leader, commits a NoOp
            await replica.DrainAsync();

            (await WaitUntilAsync(() => raftLog.SnapshotIndex == 1)).Should().BeTrue();
            raftLog.SnapshotTerm.Should().Be(1);
        }

        [Fact]
        public async Task Does_not_call_prepareSnapshot_again_while_one_is_already_in_flight() {
            var raftLog = new InMemoryRaftLog();
            var gate = new TaskCompletionSource();
            var callCount = 0;
            var config = Config("node-1", [], snapshotThresholdEntries: 0);
            var replica = new RaftReplica(new InMemoryRaftStateStore(), raftLog, new RecordingRaftTransport(), config,
                prepareSnapshot: async (_, _) => {
                    Interlocked.Increment(ref callCount);
                    await gate.Task;
                    return [];
                });
            await replica.StartAsync();

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> ready leader, commits a NoOp, triggers a snapshot
            await replica.DrainAsync();
            (await WaitUntilAsync(() => callCount == 1)).Should().BeTrue();

            // Commits a second entry, well past the threshold again — must not start a second
            // prepareSnapshot round trip while the first is still outstanding.
            await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));

            callCount.Should().Be(1);

            gate.SetResult();
            (await WaitUntilAsync(() => raftLog.SnapshotIndex > 0)).Should().BeTrue();
        }

        [Fact]
        public async Task A_failed_prepareSnapshot_clears_the_in_flight_marker_so_a_later_commit_retries() {
            var raftLog = new InMemoryRaftLog();
            var callCount = 0;
            var config = Config("node-1", [], snapshotThresholdEntries: 0);
            var replica = new RaftReplica(new InMemoryRaftStateStore(), raftLog, new RecordingRaftTransport(), config,
                prepareSnapshot: (_, _) => {
                    Interlocked.Increment(ref callCount);
                    return callCount == 1
                        ? Task.FromException<IReadOnlyList<string>>(new IOException("simulated"))
                        : Task.FromResult<IReadOnlyList<string>>([]);
                });
            await replica.StartAsync();

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> ready leader, commits a NoOp
            await replica.DrainAsync();
            (await WaitUntilAsync(() => callCount == 1)).Should().BeTrue();

            await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));

            (await WaitUntilAsync(() => callCount == 2)).Should().BeTrue();
            (await WaitUntilAsync(() => raftLog.SnapshotIndex > 0)).Should().BeTrue();
        }

        [Fact]
        public async Task Does_nothing_when_no_prepareSnapshot_delegate_is_configured() {
            var raftLog = new InMemoryRaftLog();
            var config = Config("node-1", [], snapshotThresholdEntries: 0);
            var replica = new RaftReplica(new InMemoryRaftStateStore(), raftLog, new RecordingRaftTransport(), config);
            await replica.StartAsync();

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> ready leader, commits a NoOp
            await replica.DrainAsync();
            await replica.ProposeAsync(new AppendRequest("key"u8.ToArray(), [], "payload"u8.ToArray()));

            raftLog.SnapshotIndex.Should().Be(0);
        }
    }

    public class InstallSnapshot {
        [Fact]
        public async Task Rejects_a_stale_term_with_its_own_current_term() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> term 1
            await replica.DrainAsync();

            var response = await replica.ReceiveInstallSnapshotAsync(
                new InstallSnapshotRequest { GroupId = "group-1", Term = 0, LeaderId = "node-2", LastIncludedIndex = 10, LastIncludedTerm = 1, Done = true });

            response.Term.Should().Be(replica.CurrentTerm);
        }

        [Fact]
        public async Task Reassembles_a_single_chunk_file_and_calls_installSnapshot() {
            var directory = TempStagingDirectory();
            try {
                var installCalls = new List<(IReadOnlyList<string> Paths, ulong NextOffset)>();
                var config = Config("node-1", [Peer("node-2")]) with { SnapshotStagingDirectory = directory };
                var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config,
                    installSnapshot: (paths, nextOffset, _) => {
                        installCalls.Add((paths, nextOffset));
                        return Task.CompletedTask;
                    });
                await replica.StartAsync();

                var response = await replica.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest {
                    GroupId = "group-1", Term = 1, LeaderId = "node-2",
                    LastIncludedIndex = 10, LastIncludedTerm = 1, NextLogicalOffset = 5,
                    FileName = "segment-a.sst", Data = ByteString.CopyFromUtf8("hello"), FileDone = true, Done = true,
                });

                response.Term.Should().Be(1);
                installCalls.Should().HaveCount(1);
                installCalls[0].Paths.Should().ContainSingle();
                installCalls[0].NextOffset.Should().Be(5);
                File.ReadAllText(installCalls[0].Paths[0]).Should().Be("hello");
                replica.CommitIndex.Should().Be(10);
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Reassembles_a_file_split_across_two_chunks() {
            var directory = TempStagingDirectory();
            try {
                var installCalls = new List<IReadOnlyList<string>>();
                var config = Config("node-1", [Peer("node-2")]) with { SnapshotStagingDirectory = directory };
                var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config,
                    installSnapshot: (paths, _, _) => {
                        installCalls.Add(paths);
                        return Task.CompletedTask;
                    });
                await replica.StartAsync();

                await replica.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest {
                    GroupId = "group-1", Term = 1, LeaderId = "node-2", LastIncludedIndex = 10, LastIncludedTerm = 1,
                    FileName = "segment-a.sst", Data = ByteString.CopyFromUtf8("hel"), FileDone = false, Done = false,
                });
                await replica.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest {
                    GroupId = "group-1", Term = 1, LeaderId = "node-2", LastIncludedIndex = 10, LastIncludedTerm = 1,
                    FileName = "segment-a.sst", Data = ByteString.CopyFromUtf8("lo"), FileDone = true, Done = true,
                });

                installCalls.Should().HaveCount(1);
                File.ReadAllText(installCalls[0][0]).Should().Be("hello");
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Calls_installSnapshot_with_an_empty_list_when_the_snapshot_has_no_files() {
            var directory = TempStagingDirectory();
            try {
                var installCalls = new List<IReadOnlyList<string>>();
                var config = Config("node-1", [Peer("node-2")]) with { SnapshotStagingDirectory = directory };
                var replica = new RaftReplica(new InMemoryRaftStateStore(), new InMemoryRaftLog(), new RecordingRaftTransport(), config,
                    installSnapshot: (paths, _, _) => {
                        installCalls.Add(paths);
                        return Task.CompletedTask;
                    });
                await replica.StartAsync();

                await replica.ReceiveInstallSnapshotAsync(
                    new InstallSnapshotRequest { GroupId = "group-1", Term = 1, LeaderId = "node-2", LastIncludedIndex = 10, LastIncludedTerm = 1, Done = true });

                installCalls.Should().HaveCount(1);
                installCalls[0].Should().BeEmpty();
            } finally {
                Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task Sends_an_empty_chunk_and_advances_nextIndex_when_there_are_no_segments() {
            var raftLog = new InMemoryRaftLog();
            var transport = new RecordingRaftTransport {
                OnAppendEntries = (peerId, request) => peerId == "node-2"
                    ? new AppendEntriesResponse { Term = request.Term, Success = true }
                    : throw new InvalidOperationException("node-3 never acks AppendEntries in this test"),
                OnInstallSnapshot = (_, request) => new InstallSnapshotResponse { Term = request.Term },
            };
            var config = Config("node-1", [Peer("node-2"), Peer("node-3")], snapshotThresholdEntries: 0);
            var replica = new RaftReplica(new InMemoryRaftStateStore(), raftLog, transport, config,
                prepareSnapshot: (_, _) => Task.FromResult<IReadOnlyList<string>>([]));
            await replica.StartAsync();

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> candidate, term 1
            await replica.DrainAsync();
            replica.TryPost(new VoteResponseReceived("node-2", new VoteResponse { Term = 1, VoteGranted = true }, 1));
            await replica.DrainAsync(); // -> leader (self + node-2 satisfy quorum), sends the term-opening NoOp

            // node-2 acks the NoOp — drives commit (which self-triggers a snapshot, threshold 0)
            // and advances node-2's own nextIndex; node-3 never acks, so its nextIndex stays behind.
            replica.TryPost(new AppendEntriesResponseReceived("node-2", new AppendEntriesResponse { Term = 1, Success = true }, 1, SentUpToIndex: 1));
            await replica.DrainAsync();
            (await WaitUntilAsync(() => raftLog.SnapshotIndex > 0)).Should().BeTrue("the self-triggered snapshot must run once the NoOp commits");

            // The next heartbeat retries replication to node-3 — nextIndex(1) <= SnapshotIndex now,
            // so AppendEntries can no longer serve it; InstallSnapshot must take over.
            replica.TryPost(new HeartbeatIntervalElapsed(replica.HeartbeatTimerGeneration));
            await replica.DrainAsync();

            (await WaitUntilAsync(() => transport.InstallSnapshotRequestsSent.Any(sent => sent.PeerId == "node-3"))).Should().BeTrue();
            var sent = transport.InstallSnapshotRequestsSent.First(pair => pair.PeerId == "node-3").Request;
            sent.FileName.Should().BeEmpty();
            sent.Done.Should().BeTrue();
            sent.LastIncludedIndex.Should().Be(raftLog.SnapshotIndex);

            (await WaitUntilAsync(() => replica.CommitIndex >= raftLog.SnapshotIndex)).Should().BeTrue();
        }

        [Fact]
        public async Task Does_not_start_a_second_transfer_to_the_same_peer_while_one_is_in_flight() {
            var raftLog = new InMemoryRaftLog();
            var gate = new TaskCompletionSource();
            var callCount = 0;
            var transport = new RecordingRaftTransport {
                OnAppendEntries = (peerId, request) => peerId == "node-2"
                    ? new AppendEntriesResponse { Term = request.Term, Success = true }
                    : throw new InvalidOperationException("node-3 never acks AppendEntries in this test"),
                OnInstallSnapshot = (_, request) => {
                    Interlocked.Increment(ref callCount);
                    gate.Task.GetAwaiter().GetResult(); // blocks the first transfer open
                    return new InstallSnapshotResponse { Term = request.Term };
                },
            };
            var config = Config("node-1", [Peer("node-2"), Peer("node-3")], snapshotThresholdEntries: 0);
            var replica = new RaftReplica(new InMemoryRaftStateStore(), raftLog, transport, config,
                prepareSnapshot: (_, _) => Task.FromResult<IReadOnlyList<string>>([]));
            await replica.StartAsync();

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> candidate, term 1
            await replica.DrainAsync();
            replica.TryPost(new VoteResponseReceived("node-2", new VoteResponse { Term = 1, VoteGranted = true }, 1));
            await replica.DrainAsync(); // -> leader (self + node-2 satisfy quorum), sends the term-opening NoOp

            replica.TryPost(new AppendEntriesResponseReceived("node-2", new AppendEntriesResponse { Term = 1, Success = true }, 1, SentUpToIndex: 1));
            await replica.DrainAsync();
            (await WaitUntilAsync(() => raftLog.SnapshotIndex > 0)).Should().BeTrue("the self-triggered snapshot must run once the NoOp commits");

            replica.TryPost(new HeartbeatIntervalElapsed(replica.HeartbeatTimerGeneration));
            await replica.DrainAsync();
            (await WaitUntilAsync(() => callCount >= 1)).Should().BeTrue("node-3's stale nextIndex must trigger a transfer");

            // A second heartbeat tick, while the first transfer is still gated open, must not
            // start an overlapping one to the same peer.
            replica.TryPost(new HeartbeatIntervalElapsed(replica.HeartbeatTimerGeneration));
            await replica.DrainAsync();
            await Task.Delay(20);

            callCount.Should().Be(1);

            gate.SetResult();
        }
    }

    public class ProposeConfigChangeAsync {
        [Fact]
        public async Task Throws_NotLeaderException_when_this_replica_is_not_the_ready_leader() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));

            var act = () => replica.ProposeConfigChangeAsync([Peer("node-2"), Peer("node-3")]).AsTask();

            await act.Should().ThrowAsync<NotLeaderException>();
        }

        // Becomes ready leader in a 2-node group (node-1 + node-2) via explicit message
        // injection only — no canned transport responses, so nothing can auto-commit in the
        // background and race with this test's own "not yet committed" assertions. Self-vote
        // alone is not enough here (2-member quorum is 2): node-2's vote and, separately, its
        // ack of the term-opening NoOp are both driven explicitly.
        static async Task<RaftReplica> StartAsReadyLeaderOfTwoAsync() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> candidate
            await replica.DrainAsync();
            replica.TryPost(new VoteResponseReceived("node-2", new VoteResponse { Term = 1, VoteGranted = true }, 1));
            await replica.DrainAsync(); // -> leader
            replica.TryPost(new AppendEntriesResponseReceived("node-2", new AppendEntriesResponse { Term = 1, Success = true }, 1, SentUpToIndex: 1));
            await replica.DrainAsync(); // NoOp commits -> ready leader
            replica.IsLeader.Should().BeTrue();
            return replica;
        }

        [Fact]
        public async Task Is_effective_on_append_and_resolves_the_caller_once_committed() {
            var replica = await StartAsReadyLeaderOfTwoAsync();

            var proposeTask = replica.ProposeConfigChangeAsync([Peer("node-2"), Peer("node-3")]).AsTask();
            await replica.DrainAsync();

            // Effective immediately, well before commit — the paper's rule, not gated on quorum.
            replica.ActiveConfiguration.Select(m => m.NodeId).Should().BeEquivalentTo(["node-2", "node-3"]);
            proposeTask.IsCompleted.Should().BeFalse("neither peer has acked this entry yet, and the new 3-member group needs 2 of 3");

            replica.TryPost(new AppendEntriesResponseReceived("node-2", new AppendEntriesResponse { Term = 1, Success = true }, 1, SentUpToIndex: 2));
            await replica.DrainAsync();

            await proposeTask.WaitAsync(TimeSpan.FromSeconds(5));
            proposeTask.IsCompletedSuccessfully.Should().BeTrue();
        }

        [Fact]
        public async Task Refuses_a_second_change_while_one_is_already_outstanding() {
            var replica = await StartAsReadyLeaderOfTwoAsync(); // node-2 never acks the change below — it stays pending

            var firstTask = replica.ProposeConfigChangeAsync([Peer("node-2"), Peer("node-3")]).AsTask();
            await replica.DrainAsync();

            var act = () => replica.ProposeConfigChangeAsync([Peer("node-2")]).AsTask();

            await act.Should().ThrowAsync<InvalidOperationException>();
            firstTask.IsCompleted.Should().BeFalse();
        }

        [Fact]
        public async Task A_leader_that_removes_itself_steps_down_once_the_change_commits() {
            var replica = await StartAsReadyLeaderOfTwoAsync();

            var proposeTask = replica.ProposeConfigChangeAsync([Peer("node-2")]).AsTask(); // node-1 excluded — removes itself
            await replica.DrainAsync();
            replica.ActiveConfiguration.Select(m => m.NodeId).Should().BeEquivalentTo(["node-2"]); // peer list itself is unchanged

            replica.TryPost(new AppendEntriesResponseReceived("node-2", new AppendEntriesResponse { Term = 1, Success = true }, 1, SentUpToIndex: 2));
            await replica.DrainAsync();

            await proposeTask.WaitAsync(TimeSpan.FromSeconds(5));
            replica.Role.Should().Be(ReplicaRole.Follower);
            replica.CurrentTerm.Should().Be(1, "stepping down is a plain role change, never a term bump");
        }

        [Fact]
        public async Task A_truncated_config_change_reverts_to_the_previous_configuration() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));
            var original = replica.ActiveConfiguration;

            var reply1 = new TaskCompletionSource<AppendEntriesResponse>();
            replica.TryPost(new AppendEntriesReceived(new AppendEntriesRequest {
                GroupId = "group-1", Term = 1, LeaderId = "node-3", PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommit = 0,
                Entries = { EncodeEntry(new WalRecord(WalRecordType.ShardConfigChange, 1, 1, 0, 0, ReadOnlyMemory<byte>.Empty, [],
                    SerializeFullMembership([Peer("node-4")]))) },
            }, reply1));
            await replica.DrainAsync();
            (await reply1.Task).Success.Should().BeTrue();
            replica.ActiveConfiguration.Select(m => m.NodeId).Should().BeEquivalentTo(["node-4"]);

            // A different (higher-term) leader overwrites index 1 with something else entirely —
            // the conflicting ShardConfigChange never committed, so it must revert cleanly.
            var reply2 = new TaskCompletionSource<AppendEntriesResponse>();
            replica.TryPost(new AppendEntriesReceived(new AppendEntriesRequest {
                GroupId = "group-1", Term = 2, LeaderId = "node-5", PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommit = 0,
                Entries = { EncodeEntry(new WalRecord(WalRecordType.NoOp, 2, 1, 0, 0, ReadOnlyMemory<byte>.Empty, [], ReadOnlyMemory<byte>.Empty)) },
            }, reply2));
            await replica.DrainAsync();

            (await reply2.Task).Success.Should().BeTrue();
            replica.ActiveConfiguration.Should().BeEquivalentTo(original);
        }

        [Fact]
        public async Task StartAsync_recovers_the_active_configuration_from_the_log() {
            var raftLog = new InMemoryRaftLog();
            await raftLog.AppendAsync([new WalRecord(WalRecordType.ShardConfigChange, 1, 1, 0, 0, ReadOnlyMemory<byte>.Empty, [],
                SerializeFullMembership([Peer("node-2"), Peer("node-9")]))]);

            var replica = new RaftReplica(new InMemoryRaftStateStore(), raftLog, new RecordingRaftTransport(), Config("node-1", [Peer("node-2")]));
            await replica.StartAsync();

            replica.ActiveConfiguration.Select(m => m.NodeId).Should().BeEquivalentTo(["node-2", "node-9"]);
        }

        static ReadOnlyMemory<byte> SerializeFullMembership(IReadOnlyList<RaftGroupMember> members) {
            var proto = new ShardConfiguration();
            foreach (var member in members)
                proto.Members.Add(new ShardConfigMember { NodeId = member.NodeId, Address = member.Address.ToString() });
            return proto.ToByteArray();
        }

        static ByteString EncodeEntry(WalRecord record) {
            var buffer = new byte[WalRecordSerializer.GetEncodedLength(record)];
            var written = WalRecordSerializer.Write(record, buffer);
            return ByteString.CopyFrom(buffer.AsSpan(0, written));
        }
    }

    public class StaleResponses {
        [Fact]
        public async Task A_vote_response_from_a_previous_term_is_ignored() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // -> term 1, candidate
            await replica.DrainAsync();

            replica.TryPost(new VoteResponseReceived("node-2", new VoteResponse { Term = 0, VoteGranted = true }, SentTerm: 0));
            await replica.DrainAsync();

            replica.Role.Should().Be(ReplicaRole.Candidate, "a response stamped with a stale SentTerm must never affect current state");
        }

        [Fact]
        public async Task An_append_entries_response_from_a_previous_term_is_ignored() {
            var (replica, _) = await StartAsync("node-1"); // single-node -> ready leader on its own timeout
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration));
            await replica.DrainAsync();
            var commitIndexBefore = replica.CommitIndex;

            replica.TryPost(new AppendEntriesResponseReceived("ghost-peer", new AppendEntriesResponse { Term = 0, Success = true }, SentTerm: 0, SentUpToIndex: 999));
            await replica.DrainAsync();

            replica.CommitIndex.Should().Be(commitIndexBefore, "a stale response must be discarded before it ever touches per-peer state");
        }
    }

    public class TimerGenerationStaleness {
        [Fact]
        public async Task A_stale_election_timeout_message_is_a_no_op() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"));
            var staleGeneration = replica.ElectionTimerGeneration;

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration)); // real transition -> term 1
            await replica.DrainAsync();
            var termAfterRealElection = replica.CurrentTerm;

            replica.TryPost(new ElectionTimeoutElapsed(staleGeneration)); // captured before the real transition
            await replica.DrainAsync();

            replica.CurrentTerm.Should().Be(termAfterRealElection);
        }

        [Fact]
        public async Task A_stale_heartbeat_message_is_a_no_op() {
            var (replica, _) = await StartAsync("node-1"); // single-node -> leader
            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration));
            await replica.DrainAsync();
            var termBefore = replica.CurrentTerm;

            replica.TryPost(new HeartbeatIntervalElapsed(replica.HeartbeatTimerGeneration == 0 ? ulong.MaxValue : replica.HeartbeatTimerGeneration - 1));
            await replica.DrainAsync();

            replica.Role.Should().Be(ReplicaRole.Leader);
            replica.CurrentTerm.Should().Be(termBefore);
        }
    }

    // A sister project's real term-explosion incident, as a regression test: a replica becomes
    // candidate (its election timer re-arms to generation g for the round after this one), wins
    // the election via injected votes BEFORE that g-stamped message is ever processed, and only
    // THEN does the stale ElectionTimeoutElapsed(g) arrive. It must not demote the now-leader
    // back to candidate, nor bump the term a second time.
    public class MsspRegression {
        [Fact]
        public async Task Stale_election_timer_never_demotes_a_leader_nor_bumps_the_term() {
            var (replica, _) = await StartAsync("node-1", Peer("node-2"), Peer("node-3"));

            replica.TryPost(new ElectionTimeoutElapsed(replica.ElectionTimerGeneration));
            await replica.DrainAsync(); // -> candidate, term 1; BecomeCandidateAsync already re-armed the timer to generation g
            var staleGeneration = replica.ElectionTimerGeneration;
            var termAfterBecomingCandidate = replica.CurrentTerm;

            // Win the election via injected votes before the stale timer message is processed.
            replica.TryPost(new VoteResponseReceived("node-2", new VoteResponse { Term = replica.CurrentTerm, VoteGranted = true }, replica.CurrentTerm));
            await replica.DrainAsync();
            replica.Role.Should().Be(ReplicaRole.Leader);

            // The stale firing arrives only now.
            replica.TryPost(new ElectionTimeoutElapsed(staleGeneration));
            await replica.DrainAsync();

            replica.Role.Should().Be(ReplicaRole.Leader, "a stale election-timer firing must never demote an established leader");
            replica.CurrentTerm.Should().Be(termAfterBecomingCandidate, "the term must not bump a second time from a stale timer firing");
        }
    }
}
