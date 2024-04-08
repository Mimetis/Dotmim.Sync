using System.Text;

namespace Dotmim.Sync.Extensions
{
    public static class ByteArrayExtensions
    {
        public static string ToUtf8String(this byte[] bytes) => Encoding.UTF8.GetString(bytes);
    }
}