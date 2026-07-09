using FluentAssertions;

namespace Remontoire.Raft;

public class FileRaftStateStoreTests {
    [Fact]
    public async Task LoadAsync_returns_term_zero_and_no_vote_when_nothing_was_ever_saved() {
        var directory = TempDirectory();
        try {
            var store = new FileRaftStateStore(directory);

            var state = await store.LoadAsync();

            state.Should().BeEquivalentTo(new RaftPersistentState(CurrentTerm: 0, VotedFor: null));
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task LoadAsync_throws_InvalidDataException_for_a_file_truncated_right_after_the_magic() {
        var directory = TempDirectory();
        try {
            await File.WriteAllBytesAsync(Path.Combine(directory, "raft-state.dat"), "RMTRSTAT"u8.ToArray());
            var store = new FileRaftStateStore(directory);

            var act = () => store.LoadAsync().AsTask();

            await act.Should().ThrowAsync<InvalidDataException>();
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task SaveAsync_then_LoadAsync_round_trips_every_field() {
        var directory = TempDirectory();
        try {
            var store = new FileRaftStateStore(directory);
            var saved = new RaftPersistentState(CurrentTerm: 7, VotedFor: "node-2", SnapshotNextLogicalOffset: 42, SnapshotConfiguration: [1, 2, 3, 4]);

            await store.SaveAsync(saved);
            var loaded = await store.LoadAsync();

            loaded.Should().BeEquivalentTo(saved);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task SaveAsync_then_LoadAsync_round_trips_null_VotedFor_and_null_SnapshotConfiguration() {
        var directory = TempDirectory();
        try {
            var store = new FileRaftStateStore(directory);
            var saved = new RaftPersistentState(CurrentTerm: 3, VotedFor: null, SnapshotNextLogicalOffset: 0, SnapshotConfiguration: null);

            await store.SaveAsync(saved);
            var loaded = await store.LoadAsync();

            loaded.Should().BeEquivalentTo(saved);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task SaveAsync_overwrites_a_previously_saved_state() {
        var directory = TempDirectory();
        try {
            var store = new FileRaftStateStore(directory);
            await store.SaveAsync(new RaftPersistentState(CurrentTerm: 1, VotedFor: "node-2"));

            var latest = new RaftPersistentState(CurrentTerm: 5, VotedFor: "node-3", SnapshotNextLogicalOffset: 10, SnapshotConfiguration: [9, 9]);
            await store.SaveAsync(latest);

            (await store.LoadAsync()).Should().BeEquivalentTo(latest);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task SaveAsync_creates_the_directory_when_it_does_not_exist_yet() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var store = new FileRaftStateStore(directory);
            var saved = new RaftPersistentState(CurrentTerm: 1, VotedFor: "node-1");

            await store.SaveAsync(saved);

            (await store.LoadAsync()).Should().BeEquivalentTo(saved);
        } finally {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task A_new_FileRaftStateStore_over_the_same_directory_sees_the_previously_saved_state() {
        var directory = TempDirectory();
        try {
            var saved = new RaftPersistentState(CurrentTerm: 4, VotedFor: "node-2", SnapshotNextLogicalOffset: 7, SnapshotConfiguration: [5, 6]);
            await new FileRaftStateStore(directory).SaveAsync(saved);

            var loaded = await new FileRaftStateStore(directory).LoadAsync();

            loaded.Should().BeEquivalentTo(saved);
        } finally {
            Directory.Delete(directory, recursive: true);
        }
    }

    static string TempDirectory() {
        var directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }
}
