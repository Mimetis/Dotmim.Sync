using System.Text;

namespace Dotmim.Sync.Extensions
{
    /// <summary>
    /// Byte array extensions.
    /// </summary>
    public static class ByteArrayExtensions
    {
        /// <summary>
        /// Convert a byte array to a UTF8 string.
        /// </summary>
        public static string ToUtf8String(this byte[] bytes) => Encoding.UTF8.GetString(bytes);
    }
}