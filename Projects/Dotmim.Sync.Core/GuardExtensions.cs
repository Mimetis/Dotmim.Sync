using System;
using System.Collections.Generic;
using System.Linq;

namespace Dotmim.Sync
{
    /// <summary>
    /// A simple guard clause class to provide a single entry point to all guard clauses.
    /// </summary>
    public static class Guard
    {
        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null.
        /// </summary>
        /// <typeparam name="T">Instance.</typeparam>
        /// <param name="input">Object to check.</param>
        /// <param name="message">Exception message.</param>
        public static T ThrowIfNull<T>(T input, string message = null)
        {
            if (input is null)
                throw new ArgumentNullException(nameof(input), message ?? $"Required parameter {nameof(input)} is null.");

            return input;
        }

        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null.
        /// </summary>
        public static string ThrowIfNullOrEmpty(string input, string message = null)
        {
            if (string.IsNullOrEmpty(input))
                throw new ArgumentNullException(nameof(input), message ?? $"Required parameter {nameof(input)} is null or empty.");

            return input;
        }

        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null equals to Guid.empty.
        /// </summary>
        public static Guid ThrowIfNullOrEmpty(Guid? input, string message = null)
        {
            ThrowIfNull(input, message);

            if (input == Guid.Empty)
                throw new ArgumentException(message ?? $"Required Guid parameter {nameof(input)} is empty.", nameof(input));

            return input.Value;
        }

        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null or enumerable is empty.
        /// </summary>
        public static IEnumerable<T> ThrowIfNullOrEmpty<T>(IEnumerable<T> input, string message = null)
        {
            ThrowIfNull(input, message);

            if (!input.Any())
                throw new ArgumentException(message ?? $"Required Enumerable {nameof(input)} is empty.", nameof(input));

            return input;
        }

        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null or empty or whitespace.
        /// </summary>
        public static string ThrowIfNullOrWhiteSpace(string input, string message = null)
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new ArgumentException(message ?? $"Required String parameter {nameof(input)} is null or empty.", nameof(input));

            return input;
        }

        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is equals to Default.
        /// </summary>
        public static T ThrowIfDefault<T>(T input, string message = null)
        {
            if (EqualityComparer<T>.Default.Equals(input, default!) || input is null)
                throw new ArgumentException(message ?? $"Parameter [{nameof(input)}] is default value for type {typeof(T).Name}", nameof(input));

            return input;
        }
    }
}