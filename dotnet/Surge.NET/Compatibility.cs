// Polyfills for netstandard2.0 compatibility

#if !NET5_0_OR_GREATER
// Required for 'init' accessors on netstandard2.0
namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit { }
}
#endif

#if NETSTANDARD2_0
namespace Surge
{
    using System;
    using System.Runtime.InteropServices;
    using System.Text;

    internal static class MarshalHelper
    {
        /// <summary>
        /// Read a UTF-8 string from an unmanaged pointer.
        /// Polyfill for Marshal.PtrToStringUTF8 which is unavailable on netstandard2.0.
        /// </summary>
        internal static string? PtrToStringUTF8(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
                return null;

            // Find null terminator
            int length = 0;
            while (Marshal.ReadByte(ptr, length) != 0)
                length++;

            if (length == 0)
                return string.Empty;

            var buffer = new byte[length];
            Marshal.Copy(ptr, buffer, 0, length);
            return Encoding.UTF8.GetString(buffer);
        }
    }
}
#endif
