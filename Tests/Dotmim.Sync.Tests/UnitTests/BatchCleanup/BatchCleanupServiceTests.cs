using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class BatchCleanupServiceTests : IDisposable
    {
        private ITest test;
        private Stopwatch stopwatch;
        private readonly string tempDirectory;
        private readonly BatchCleanupService batchCleanupService;

        public ITestOutputHelper Output { get; }

        public BatchCleanupServiceTests(ITestOutputHelper output)
        {
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);
            this.stopwatch = Stopwatch.StartNew();

            this.tempDirectory = Path.Combine(Path.GetTempPath(), $"BatchCleanupTest_{Guid.NewGuid():N}");
            Directory.CreateDirectory(this.tempDirectory);

            this.batchCleanupService = new BatchCleanupService();
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithSyncOptions_ShouldUseOptionsValues()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch1");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            var options = new SyncOptions
            {
                BatchDirectory = batchDir,
                BatchRetentionPeriod = retentionPeriod
            };

            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));

            var result = await this.batchCleanupService.CleanupExpiredBatchesAsync(options);

            Assert.Equal(1, result);
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithNullBatchDirectory_ShouldThrowArgumentException()
        {
            var retentionPeriod = TimeSpan.FromHours(1);

            var exceptionNull = await Assert.ThrowsAsync<ArgumentException>(
                () => this.batchCleanupService.CleanupExpiredBatchesAsync((string)null, retentionPeriod));

            Assert.Equal("batchDirectory", exceptionNull.ParamName);
        }
        
        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithEmptyBatchDirectory_ShouldThrowArgumentException()
        {
            var retentionPeriod = TimeSpan.FromHours(1);

            var exceptionEmpty = await Assert.ThrowsAsync<ArgumentException>(
                () => this.batchCleanupService.CleanupExpiredBatchesAsync("", retentionPeriod));

            Assert.Equal("batchDirectory", exceptionEmpty.ParamName);
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithNonExistentDirectory_ShouldReturnZero()
        {
            var nonExistentPath = Path.Combine(this.tempDirectory, "nonexistent");
            var retentionPeriod = TimeSpan.FromHours(1);

            var result = await this.batchCleanupService.CleanupExpiredBatchesAsync(nonExistentPath, retentionPeriod);

            Assert.Equal(0, result);
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithZeroRetentionPeriod_ShouldReturnZero()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch2");
            Directory.CreateDirectory(batchDir);
            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));

            var result = await this.batchCleanupService.CleanupExpiredBatchesAsync(batchDir, TimeSpan.Zero);

            Assert.Equal(0, result);
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithExpiredBatches_ShouldCleanupCorrectly()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch3");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);

            var expiredDir1 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));
            var expiredDir2 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-3));
            var validDir = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddMinutes(-30));

            var result = await this.batchCleanupService.CleanupExpiredBatchesAsync(batchDir, retentionPeriod);

            Assert.Equal(2, result);
            Assert.False(Directory.Exists(expiredDir1));
            Assert.False(Directory.Exists(expiredDir2));
            Assert.True(Directory.Exists(validDir));
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithNoExpiredBatches_ShouldReturnZero()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch4");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);

            var validDir1 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddMinutes(-30));
            var validDir2 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddMinutes(-45));

            var result = await this.batchCleanupService.CleanupExpiredBatchesAsync(batchDir, retentionPeriod);

            Assert.Equal(0, result);
            Assert.True(Directory.Exists(validDir1));
            Assert.True(Directory.Exists(validDir2));
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithInvalidDirectoryNames_ShouldIgnoreInvalidOnes()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch5");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);

            var invalidDir1 = Path.Combine(batchDir, "invalid_name");
            var invalidDir2 = Path.Combine(batchDir, "20240101abc");
            var invalidDir3 = Path.Combine(batchDir, "202401");
            var expiredDir = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));

            Directory.CreateDirectory(invalidDir1);
            Directory.CreateDirectory(invalidDir2);
            Directory.CreateDirectory(invalidDir3);

            var result = await this.batchCleanupService.CleanupExpiredBatchesAsync(batchDir, retentionPeriod);

            Assert.Equal(1, result);
            Assert.True(Directory.Exists(invalidDir1));
            Assert.True(Directory.Exists(invalidDir2));
            Assert.True(Directory.Exists(invalidDir3));
            Assert.False(Directory.Exists(expiredDir));
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithNestedFiles_ShouldDeleteRecursively()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch6");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            var expiredDir = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));

            var nestedDir = Path.Combine(expiredDir, "nested");
            Directory.CreateDirectory(nestedDir);
            File.WriteAllText(Path.Combine(expiredDir, "file1.txt"), "test content");
            File.WriteAllText(Path.Combine(nestedDir, "file2.txt"), "nested content");

            var result = await this.batchCleanupService.CleanupExpiredBatchesAsync(batchDir, retentionPeriod);

            Assert.Equal(1, result);
            Assert.False(Directory.Exists(expiredDir));
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithCancellationToken_ShouldRespectCancellation()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch7");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAsync<TaskCanceledException>(
                () => this.batchCleanupService.CleanupExpiredBatchesAsync(batchDir, retentionPeriod, cts.Token));
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithLockedDirectory_ShouldContinueWithOthers()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch8");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            var expiredDir1 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));
            var expiredDir2 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-3));

            File.WriteAllText(Path.Combine(expiredDir1, "file.txt"), "content");
            using var fileStream = File.Open(Path.Combine(expiredDir1, "file.txt"), FileMode.Open, FileAccess.Read, FileShare.None);

            var result = await this.batchCleanupService.CleanupExpiredBatchesAsync(batchDir, retentionPeriod);

            Assert.Equal(1, result);
            Assert.True(Directory.Exists(expiredDir1));
            Assert.False(Directory.Exists(expiredDir2));
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithSyncOptions_ShouldUseOptionsValues()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch9");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            var options = new SyncOptions
            {
                BatchDirectory = batchDir,
                BatchRetentionPeriod = retentionPeriod
            };

            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(options, retentionPeriod);

            Assert.Single(result);
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithNullBatchDirectory_ShouldThrowArgumentException()
        {
            var retentionPeriod = TimeSpan.FromHours(1);

            var exceptionNull = await Assert.ThrowsAsync<ArgumentException>(
                () => this.batchCleanupService.GetExpiredBatchDirectoriesAsync((string)null, retentionPeriod));

            Assert.Equal("batchDirectory", exceptionNull.ParamName);
        }
        
        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithEmptyBatchDirectory_ShouldThrowArgumentException()
        {
            var retentionPeriod = TimeSpan.FromHours(1);

            var exceptionEmpty = await Assert.ThrowsAsync<ArgumentException>(
                () => this.batchCleanupService.GetExpiredBatchDirectoriesAsync("", retentionPeriod));

            Assert.Equal("batchDirectory", exceptionEmpty.ParamName);
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithNonExistentDirectory_ShouldReturnEmptyList()
        {
            var nonExistentPath = Path.Combine(this.tempDirectory, "nonexistent");
            var retentionPeriod = TimeSpan.FromHours(1);

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(nonExistentPath, retentionPeriod);

            Assert.Empty(result);
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithZeroRetentionPeriod_ShouldReturnEmptyList()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch10");
            Directory.CreateDirectory(batchDir);
            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(batchDir, TimeSpan.Zero);

            Assert.Empty(result);
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithExpiredBatches_ShouldReturnCorrectList()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch11");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);

            var expiredDir1 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));
            var expiredDir2 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-3));
            var validDir = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddMinutes(-30));

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(batchDir, retentionPeriod);

            Assert.Equal(2, result.Count);
            Assert.Contains(expiredDir1, result);
            Assert.Contains(expiredDir2, result);
            Assert.DoesNotContain(validDir, result);
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithNoExpiredBatches_ShouldReturnEmptyList()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch12");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);

            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddMinutes(-30));
            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddMinutes(-45));

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(batchDir, retentionPeriod);

            Assert.Empty(result);
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithInvalidDirectoryNames_ShouldIgnoreInvalidOnes()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch13");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);

            var invalidDir1 = Path.Combine(batchDir, "invalid_name");
            var invalidDir2 = Path.Combine(batchDir, "20240101abc");
            var invalidDir3 = Path.Combine(batchDir, "202401");
            var expiredDir = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));

            Directory.CreateDirectory(invalidDir1);
            Directory.CreateDirectory(invalidDir2);
            Directory.CreateDirectory(invalidDir3);

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(batchDir, retentionPeriod);

            Assert.Single(result);
            Assert.Contains(expiredDir, result);
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_ShouldReturnDirectoriesInOrder()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch14");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);

            var expiredDir1 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-5), "202401011200_test1");
            var expiredDir2 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-3), "202401011000_test2");
            var expiredDir3 = CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-4), "202401011100_test3");

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(batchDir, retentionPeriod);

            Assert.Equal(3, result.Count);
            Assert.Equal(Path.GetFileName(expiredDir2), Path.GetFileName(result[0]));
            Assert.Equal(Path.GetFileName(expiredDir3), Path.GetFileName(result[1]));
            Assert.Equal(Path.GetFileName(expiredDir1), Path.GetFileName(result[2]));
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithDirectoryAccessException_ShouldReturnEmptyList()
        {
            var nonAccessibleDir = Path.Combine(this.tempDirectory, "restricted");
            Directory.CreateDirectory(nonAccessibleDir);

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(nonAccessibleDir, TimeSpan.FromHours(1));

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public async Task GetExpiredBatchDirectoriesAsync_WithEdgeCaseTimestamps_ShouldHandleCorrectly()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch15");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            var cutoffTime = DateTime.UtcNow - retentionPeriod;

            // Test directories at minute-level boundaries since format is yyyyMMddHHmm
            var exactlyCutoffDir = CreateTestBatchDirectory(batchDir, cutoffTime);
            var oneMinuteBeforeCutoffDir = CreateTestBatchDirectory(batchDir, cutoffTime.AddMinutes(-1));
            var oneMinuteAfterCutoffDir = CreateTestBatchDirectory(batchDir, cutoffTime.AddMinutes(1));

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(batchDir, retentionPeriod);

            // Only directories older than cutoff should be included
            Assert.Equal(2, result.Count);
            Assert.Contains(exactlyCutoffDir, result);
            Assert.Contains(oneMinuteBeforeCutoffDir, result);
            Assert.DoesNotContain(oneMinuteAfterCutoffDir, result);
        }

        [Theory]
        [InlineData("202401011200")]
        [InlineData("202401011200_additional")]
        [InlineData("202401011200_test_batch")]
        [InlineData("202401011200abcdef")]
        public async Task GetExpiredBatchDirectoriesAsync_WithVariousValidFormats_ShouldIncludeAll(string directoryName)
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch_format_test");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            var targetDir = Path.Combine(batchDir, directoryName);
            Directory.CreateDirectory(targetDir);

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(batchDir, retentionPeriod);

            Assert.Single(result);
            Assert.Contains(targetDir, result);
        }

        [Theory]
        [InlineData("2024010112")]
        [InlineData("20240101120")]
        [InlineData("not_a_timestamp")]
        [InlineData("2024a1011200")]
        [InlineData("")]
        public async Task GetExpiredBatchDirectoriesAsync_WithInvalidFormats_ShouldIgnore(string directoryName)
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch_invalid_test");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            var targetDir = Path.Combine(batchDir, directoryName);
            if (!string.IsNullOrEmpty(directoryName))
                Directory.CreateDirectory(targetDir);

            var result = await this.batchCleanupService.GetExpiredBatchDirectoriesAsync(batchDir, retentionPeriod);

            Assert.Empty(result);
        }

        [Fact]
        public async Task CleanupExpiredBatchesAsync_WithCancellationDuringCleanup_ShouldStopGracefully()
        {
            var batchDir = Path.Combine(this.tempDirectory, "batch_cancellation");
            Directory.CreateDirectory(batchDir);

            var retentionPeriod = TimeSpan.FromHours(1);
            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-2));
            CreateTestBatchDirectory(batchDir, DateTime.UtcNow.AddHours(-3));

            using var cts = new CancellationTokenSource();
            cts.CancelAfter(1);

            await Assert.ThrowsAsync<TaskCanceledException>(
                () => this.batchCleanupService.CleanupExpiredBatchesAsync(batchDir, retentionPeriod, cts.Token));
        }

        private string CreateTestBatchDirectory(string parentDir, DateTime timestamp, string customName = null)
        {
            var directoryName = customName ?? timestamp.ToString("yyyyMMddHHmm", CultureInfo.InvariantCulture) + "_test";
            var fullPath = Path.Combine(parentDir, directoryName);
            Directory.CreateDirectory(fullPath);
            return fullPath;
        }

        public void Dispose()
        {
            this.stopwatch?.Stop();

            if (Directory.Exists(this.tempDirectory))
            {
                try
                {
                    Directory.Delete(this.tempDirectory, true);
                }
                catch
                {
                }
            }

            this.Output?.WriteLine($"Test {this.test?.DisplayName} took {this.stopwatch?.Elapsed.ToString() ?? "unknown time"}");
        }
    }
}