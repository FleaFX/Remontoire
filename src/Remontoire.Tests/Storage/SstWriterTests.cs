using System.Text;
using FluentAssertions;

namespace Remontoire.Storage;

public class SstWriterTests {
    public class WriteAsync {
        [Fact]
        public async Task Throws_when_there_are_no_entries() {
            var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

            var act = async () => await SstWriter.WriteAsync(path, []);

            await act.Should().ThrowAsync<ArgumentException>();
        }

        [Fact]
        public async Task Creates_the_final_file_and_removes_the_temp_file() {
            var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try {
                await SstWriter.WriteAsync(path, [SampleEntry(1)]);

                File.Exists(path).Should().BeTrue();
                File.Exists(path + ".tmp").Should().BeFalse();
            } finally {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task Throws_when_a_leftover_temp_file_from_a_previous_write_still_exists() {
            var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            await File.WriteAllBytesAsync(path + ".tmp", [0]);
            try {
                var act = async () => await SstWriter.WriteAsync(path, [SampleEntry(1)]);

                await act.Should().ThrowAsync<IOException>();
            } finally {
                File.Delete(path + ".tmp");
            }
        }
    }

    static LogEntry SampleEntry(ulong logicalOffset) =>
        new(logicalOffset, TimestampMicros: 42, Encoding.UTF8.GetBytes("order-42"), [], Encoding.UTF8.GetBytes("hello world"));
}
