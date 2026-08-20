using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Surge.Tests
{
    [CollectionDefinition(Name, DisableParallelization = true)]
    public sealed class ResetCancelCompatibilityGroup
    {
        public const string Name = "Reset cancel compatibility";
    }

    [Collection(ResetCancelCompatibilityGroup.Name)]
    public sealed class ResetCancelCompatTests : IDisposable
    {
        public ResetCancelCompatTests() => NativeMethods.ResetResetCancelProbeForTesting();

        public void Dispose() => NativeMethods.ResetResetCancelProbeForTesting();

        [Fact]
        public void TryResetCancel_NullContext_DoesNotInvokeNativeMethod()
        {
            var calls = 0;

            var supported = NativeMethods.TryResetCancelCore(
                IntPtr.Zero,
                _ =>
                {
                    calls++;
                    return 0;
                },
                out var result);

            Assert.False(supported);
            Assert.Equal(0, result);
            Assert.Equal(0, calls);
        }

        [Fact]
        public void TryResetCancel_MissingSymbol_IsCached()
        {
            var calls = 0;
            int MissingSymbol(IntPtr _)
            {
                calls++;
                throw new EntryPointNotFoundException("surge_reset_cancel");
            }

            Assert.False(NativeMethods.TryResetCancelCore(new IntPtr(1), MissingSymbol, out var firstResult));
            Assert.False(NativeMethods.TryResetCancelCore(new IntPtr(1), MissingSymbol, out var secondResult));

            Assert.Equal(0, firstResult);
            Assert.Equal(0, secondResult);
            Assert.Equal(1, calls);
        }

        [Fact]
        public void TryResetCancel_AvailableSymbol_ReturnsEveryNativeResult()
        {
            var nextResult = 0;
            int Invoke(IntPtr _) => nextResult--;

            Assert.True(NativeMethods.TryResetCancelCore(new IntPtr(1), Invoke, out var firstResult));
            Assert.True(NativeMethods.TryResetCancelCore(new IntPtr(1), Invoke, out var secondResult));

            Assert.Equal(0, firstResult);
            Assert.Equal(-1, secondResult);
        }

        [Fact]
        public void TryResetCancel_DllNotFoundException_IsNotHidden()
        {
            Assert.Throws<DllNotFoundException>(() =>
                NativeMethods.TryResetCancelCore(
                    new IntPtr(1),
                    _ => throw new DllNotFoundException("surge"),
                    out _));
        }

        [Fact]
        public void TryResetCancel_OtherTypeLoadException_IsNotHidden()
        {
            Assert.Throws<TypeLoadException>(() =>
                NativeMethods.TryResetCancelCore(
                    new IntPtr(1),
                    _ => throw new TypeLoadException("unexpected"),
                    out _));
        }

        [Fact]
        public async Task TryResetCancel_ConcurrentMissingSymbol_IsProbedOnce()
        {
            const int taskCount = 8;
            var calls = 0;
            using var entered = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();

            int MissingSymbol(IntPtr _)
            {
                Interlocked.Increment(ref calls);
                entered.Set();
                release.Wait();
                throw new EntryPointNotFoundException("surge_reset_cancel");
            }

            var tasks = new Task<bool>[taskCount];
            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() =>
                    NativeMethods.TryResetCancelCore(new IntPtr(1), MissingSymbol, out _));
            }

            try
            {
                Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
            }
            finally
            {
                release.Set();
            }

            var results = await Task.WhenAll(tasks);
            Assert.All(results, Assert.False);
            Assert.Equal(1, calls);
        }
    }
}
