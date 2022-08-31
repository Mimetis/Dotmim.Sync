using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Calculate a hash for each BatchInfo
    /// Original source code from @gentledepp
    /// </summary>
    public static class HashAlgorithm
    {
        /// <summary>
        /// Create a hash with SHA256
        /// </summary>
        public static class SHA256
        {
            public static void EnsureHash(Stream readableStream, string hashToCompare)
            {
                readableStream.Seek(0, SeekOrigin.Begin);

                var hash = Create(readableStream);
                var hashString = Convert.ToBase64String(hash);

                if (!hashString.Equals(hashToCompare, StringComparison.InvariantCultureIgnoreCase))
                    throw new SyncHashException(hashToCompare, hashString);

                readableStream.Seek(0, SeekOrigin.Begin);
            }

            public static bool CompareHash(byte[] hash1, byte[] hash2)
            {
                var hash1String = Convert.ToBase64String(hash1);
                var hash2String = Convert.ToBase64String(hash2);

                if (!hash1String.Equals(hash2String, StringComparison.InvariantCultureIgnoreCase))
                    return false;

                return true;
            }

            public static byte[] Create(string str)
            {
                var data = Convert.FromBase64String(str);
                return Create(data);
            }

            public static byte[] Create(byte[] data)
            {
                using var stream = new MemoryStream(data);
                var b = Create(stream);
                //stream.Flush();

                return b;
            }

            public static byte[] Create(Stream stream)
            {
                using var sha256 = System.Security.Cryptography.SHA256.Create();
                return sha256.ComputeHash(stream);
            }
        }
    }
}
