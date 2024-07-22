using System;
using System.IO;

namespace Dotmim.Sync
{
    /// <summary>
    /// Calculate a hash for each BatchInfo
    /// Original source code from @gentledepp.
    /// </summary>
    public static class HashAlgorithm
    {
        /// <summary>
        /// Create a hash with SHA256.
        /// </summary>
#pragma warning disable CA1034 // Nested types should not be visible
        public static class SHA256
#pragma warning restore CA1034 // Nested types should not be visible
        {
            /// <summary>
            /// Ensure the hash of the stream is the same as the hashToCompare.
            /// </summary>
            public static void EnsureHash(Stream readableStream, string hashToCompare)
            {
                Guard.ThrowIfNull(readableStream);

                readableStream.Seek(0, SeekOrigin.Begin);

                var hash = Create(readableStream);
                var hashString = Convert.ToBase64String(hash);

                if (!hashString.Equals(hashToCompare, StringComparison.OrdinalIgnoreCase))
                    throw new SyncHashException(hashToCompare, hashString);

                readableStream.Seek(0, SeekOrigin.Begin);
            }

            /// <summary>
            /// Compare two hashes.
            /// </summary>
            public static bool CompareHash(byte[] hash1, byte[] hash2)
            {
                var hash1String = Convert.ToBase64String(hash1);
                var hash2String = Convert.ToBase64String(hash2);

                if (!hash1String.Equals(hash2String, StringComparison.OrdinalIgnoreCase))
                    return false;

                return true;
            }

            /// <summary>
            /// Create a hash from a string.
            /// </summary>
            public static byte[] Create(string str)
            {
                var data = Convert.FromBase64String(str);
                return Create(data);
            }

            /// <summary>
            /// Create a hash from a byte array.
            /// </summary>
            public static byte[] Create(byte[] data)
            {
                using var stream = new MemoryStream(data);
                var b = Create(stream);

                // stream.Flush();
                return b;
            }

            /// <summary>
            /// Create a hash from a stream.
            /// </summary>
            public static byte[] Create(Stream stream)
            {
                using var sha256 = System.Security.Cryptography.SHA256.Create();
                return sha256.ComputeHash(stream);
            }
        }
    }
}