using FluentAssertions;

namespace Remontoire.Storage;

public class SingleWriterGuardTests {
    public class Enter {
        [Fact]
        public void Succeeds_when_not_currently_entered() {
            var guard = new SingleWriterGuard();

            var act = guard.Enter;

            act.Should().NotThrow();
        }

        [Fact]
        public void Throws_when_already_entered() {
            var guard = new SingleWriterGuard();
            guard.Enter();

            var act = guard.Enter;

            act.Should().Throw<InvalidOperationException>();
        }

        [Fact]
        public void Succeeds_again_after_the_previous_scope_was_disposed() {
            var guard = new SingleWriterGuard();
            guard.Enter().Dispose();

            var act = guard.Enter;

            act.Should().NotThrow();
        }

        [Fact]
        public void Can_be_entered_and_exited_repeatedly_in_sequence() {
            var guard = new SingleWriterGuard();

            for (var i = 0; i < 5; i++)
                guard.Enter().Dispose();
        }
    }
}
