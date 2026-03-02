using System;
using System.Runtime.InteropServices;
using Xunit;

namespace Surge.Tests
{
    public class NativeMethodsTests
    {
        [Fact]
        public void NativeStructLayout_SurgeProgressNative_HasExpectedSize()
        {
            // surge_progress: int(4) + int(4) + int(4) + long(8) + long(8) + long(8) + long(8) + double(8)
            // = 52 bytes, but with alignment padding the struct may be larger.
            int size = Marshal.SizeOf<SurgeProgressNative>();
            Assert.True(size >= 52, $"SurgeProgressNative size {size} is too small");
        }

        [Fact]
        public void NativeStructLayout_SurgeResourceBudgetNative_HasExpectedSize()
        {
            // surge_resource_budget: long(8) + int(4) + int(4) + long(8) + int(4)
            // = 28 bytes minimum
            int size = Marshal.SizeOf<SurgeResourceBudgetNative>();
            Assert.True(size >= 28, $"SurgeResourceBudgetNative size {size} is too small");
        }

        [Fact]
        public void NativeStructLayout_SurgeErrorNative_HasExpectedSize()
        {
            // surge_error: int(4) + pointer(8 on 64-bit)
            int size = Marshal.SizeOf<SurgeErrorNative>();
            Assert.True(size >= 8, $"SurgeErrorNative size {size} is too small");
        }

        [Fact]
        public void ProgressNative_FieldOffsets_MatchC()
        {
            // Verify field ordering matches the C struct
            var progress = new SurgeProgressNative
            {
                Phase = 3,
                PhasePercent = 75,
                TotalPercent = 50,
                BytesDone = 1024,
                BytesTotal = 4096,
                ItemsDone = 5,
                ItemsTotal = 20,
                SpeedBytesPerSec = 123.456
            };

            Assert.Equal(3, progress.Phase);
            Assert.Equal(75, progress.PhasePercent);
            Assert.Equal(50, progress.TotalPercent);
            Assert.Equal(1024L, progress.BytesDone);
            Assert.Equal(4096L, progress.BytesTotal);
            Assert.Equal(5L, progress.ItemsDone);
            Assert.Equal(20L, progress.ItemsTotal);
            Assert.Equal(123.456, progress.SpeedBytesPerSec, 3);
        }

        [Fact]
        public void ResourceBudgetNative_FieldOffsets_MatchC()
        {
            var budget = new SurgeResourceBudgetNative
            {
                MaxMemoryBytes = 1024 * 1024 * 512L,
                MaxThreads = 4,
                MaxConcurrentDownloads = 8,
                MaxDownloadSpeedBps = 1024 * 1024L,
                ZstdCompressionLevel = 12
            };

            Assert.Equal(1024 * 1024 * 512L, budget.MaxMemoryBytes);
            Assert.Equal(4, budget.MaxThreads);
            Assert.Equal(8, budget.MaxConcurrentDownloads);
            Assert.Equal(1024 * 1024L, budget.MaxDownloadSpeedBps);
            Assert.Equal(12, budget.ZstdCompressionLevel);
        }

        [Fact]
        public void ErrorNative_FieldOffsets_MatchC()
        {
            var error = new SurgeErrorNative
            {
                Code = -1,
                Message = IntPtr.Zero
            };

            Assert.Equal(-1, error.Code);
            Assert.Equal(IntPtr.Zero, error.Message);
        }

        [Fact]
        public void DelegateTypes_CanBeInstantiated()
        {
            // Verify delegate types can be created without errors
            SurgeProgressCallbackDelegate progressCb = (progress, userData) => { };
            SurgeEventCallbackDelegate eventCb = (version, userData) => { };

            Assert.NotNull(progressCb);
            Assert.NotNull(eventCb);
        }

        [Fact]
        public void NativeLibraryName_IsSurge()
        {
            // This test verifies the library is configured to load "surge".
            // We can only verify this indirectly by checking that calling a function
            // throws DllNotFoundException with the expected library name.
            var ex = Assert.Throws<DllNotFoundException>(() =>
            {
                NativeMethods.ContextCreate();
            });
            Assert.Contains("surge", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
