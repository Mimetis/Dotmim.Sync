using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Dotmim.Sync
{

    /// <summary>
    /// Simple interface to provide a generic mechanism to build guard clause extension methods from.
    /// </summary>
    public interface IGuardClause
    {
    }

    public class Guard : IGuardClause
    {
        /// <summary>
        /// An entry point to a set of Guard Clauses.
        /// </summary>
        public static IGuardClause Against { get; } = new Guard();

        private Guard() { }
    }

    /// <summary>
    /// A collection of common guard clauses, implemented as extensions.
    /// </summary>
    /// <example>
    /// Guard.Against.Null(input, nameof(input));
    /// </example>
    public static partial class GuardClauseExtensions
    {
        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null.
        /// </summary>
        public static T Null<T>(this IGuardClause guardClause, T input, string message = null)
        {
            if (input is null)
            {
                var parameterName = nameof(input);

                if (string.IsNullOrEmpty(message))
                    throw new ArgumentException(message ?? $"Required parameter {parameterName} is null.", parameterName);

                throw new ArgumentNullException(parameterName, message);
            }

            return input;
        }

        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null.
        /// </summary>
        public static string NullOrEmpty(this IGuardClause guardClause, string input, string message = null)
        {
            Guard.Against.Null(input, message);
            if (input == string.Empty)
            {
                var parameterName = nameof(input);

                throw new ArgumentException(message ?? $"Required parameter {parameterName} is empty.", parameterName);
            }

            return input;
        }


        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null equals to Guid.Empty.
        /// </summary>
        public static Guid NullOrEmpty(this IGuardClause guardClause, Guid? input, string message = null)
        {
            Guard.Against.Null(input, message);
            if (input == Guid.Empty)
            {
                var parameterName = nameof(input);

                throw new ArgumentException(message ?? $"Required Guid parameter {parameterName} is empty.", parameterName);
            }

            return input.Value;
        }


        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null or enumerable is empty.
        /// </summary>
        public static IEnumerable<T> NullOrEmpty<T>(this IGuardClause guardClause, IEnumerable<T> input, string message = null)
        {
            Guard.Against.Null(input, message);
            if (!input.Any())
            {
                var parameterName = nameof(input);

                throw new ArgumentException(message ?? $"Required Enumerable {parameterName} is empty.", parameterName);
            }

            return input;
        }



        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is null or empty or whitespace.
        /// </summary>
        public static string NullOrWhiteSpace(this IGuardClause guardClause, string input, string message = null)
        {
            Guard.Against.NullOrEmpty(input, message);
            if (string.IsNullOrWhiteSpace(input))
            {
                var parameterName = nameof(input);

                throw new ArgumentException(message ?? $"Required String parameter {parameterName} is empty.", parameterName);
            }

            return input;
        }

        /// <summary>
        /// Throws an <see cref="ArgumentNullException" /> if <paramref name="input" /> is equals to Default.
        /// </summary>
        public static T Default<T>(this IGuardClause guardClause, T input, string message = null)
        {
            if (EqualityComparer<T>.Default.Equals(input, default!) || input is null)
            {
                var parameterName = nameof(input);

                throw new ArgumentException(message ?? $"Parameter [{parameterName}] is default value for type {typeof(T).Name}", parameterName);
            }

            return input;
        }
    }
}
