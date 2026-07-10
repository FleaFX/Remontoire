using FluentAssertions;

namespace Remontoire.Server;

public class MigrationAdmissionGateTests {
    [Fact]
    public void A_group_is_not_paused_by_default() {
        var gate = new MigrationAdmissionGate();

        gate.IsPaused("group-1").Should().BeFalse();
    }

    [Fact]
    public void Pause_marks_a_group_as_paused() {
        var gate = new MigrationAdmissionGate();

        gate.Pause("group-1");

        gate.IsPaused("group-1").Should().BeTrue();
    }

    [Fact]
    public void Resume_clears_a_paused_group() {
        var gate = new MigrationAdmissionGate();
        gate.Pause("group-1");

        gate.Resume("group-1");

        gate.IsPaused("group-1").Should().BeFalse();
    }

    [Fact]
    public void Pausing_one_group_does_not_affect_another() {
        var gate = new MigrationAdmissionGate();

        gate.Pause("group-1");

        gate.IsPaused("group-2").Should().BeFalse();
    }
}
