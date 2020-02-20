using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Web.Client
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

            public static byte[] Create(byte[] data)
            {
                using (var stream = new MemoryStream(data))
                {
                    return Create(stream);
                }
            }

            public static byte[] Create(Stream stream)
            {
                using (var sha256 = System.Security.Cryptography.SHA256.Create())
                {
                    return sha256.ComputeHash(stream);
                }
            }
        }
    }
}
