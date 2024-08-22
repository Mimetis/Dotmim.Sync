using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Dotmim.Sync
{

    /// <summary>
    /// Default logger used in Dotmim.Sync. This logger is synchronous and can log to console and debug output window.
    /// </summary>
    public class SyncLogger : ILogger, IDisposable
    {

        /// <summary>
        /// Gets a value indicating the minimum LogLevel value.
        /// </summary>
        public LogLevel MinimumLevel { get; internal set; }

        /// <summary>
        /// Gets the output writers to write log messages.
        /// </summary>
        internal List<OutputWriter> OutputWriters { get; } = [];

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncLogger"/> class.
        /// </summary>
        public SyncLogger() => this.MinimumLevel = LogLevel.Error;

        /// <summary>
        /// Adds an output to console when logging something.
        /// </summary>
        public SyncLogger AddConsole()
        {
            if (!this.OutputWriters.Any(w => w.Name == "Console"))
                this.OutputWriters.Add(new ConsoleWriter());

            return this;
        }

        /// <summary>
        /// Adds an output to diagnostics debug window when logging something.
        /// </summary>
        public SyncLogger AddDebug()
        {
            if (!this.OutputWriters.Any(w => w.Name == "Debug"))
                this.OutputWriters.Add(new DebugWriter());

            return this;
        }

        /// <summary>
        /// Adds minimum level : 0 Trace, 1 Debug, 2 Information, 3, Warning, 4 Error, 5 Critical, 6 None.
        /// </summary>
        public SyncLogger SetMinimumLevel(LogLevel minimumLevel)
        {
            this.MinimumLevel = minimumLevel;
            return this;
        }

        /// <summary>
        /// Begin a new scope.
        /// </summary>
        public IDisposable BeginScope<TState>(TState state) => this;

        /// <summary>
        /// Gets if the logger can log something, according to the minimum log level parameterized.
        /// </summary>
        public bool IsEnabled(LogLevel logLevel) => this.MinimumLevel <= logLevel;

        /// <summary>
        /// Log to all output writers configured.
        /// </summary>
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!this.IsEnabled(logLevel))
                return;

            if (this.OutputWriters.Count <= 0)
                return;

            var now = DateTime.UtcNow.TimeOfDay.ToString(@"hh\:mm\:ss\.ff", CultureInfo.InvariantCulture);
            string message = string.Empty;
            if (formatter != null && state is string)
                message = formatter(state, exception) ?? string.Empty;

            var levelColors = GetLogLevelConsoleColors(logLevel);

            foreach (var outputWriter in this.OutputWriters)
            {
                Write(outputWriter, "[", ConsoleColor.Black, ConsoleColor.DarkGray);
                Write(outputWriter, now, ConsoleColor.Black, ConsoleColor.White);
                Write(outputWriter, "]", ConsoleColor.Black, ConsoleColor.DarkGray);
                Write(outputWriter, " ");

                Write(outputWriter, $"{GetLogLevelString(logLevel)}", levelColors.Background, levelColors.Foreground);
                Write(outputWriter, ": ");
                Write(outputWriter, "[", ConsoleColor.Black, ConsoleColor.DarkGray);
                Write(outputWriter, string.IsNullOrEmpty(eventId.Name) ? eventId.Id.ToString(CultureInfo.InvariantCulture) : eventId.Name, ConsoleColor.Black, ConsoleColor.White);
                Write(outputWriter, "]", ConsoleColor.Black, ConsoleColor.DarkGray);
                Write(outputWriter, " ");
                WriteLine(outputWriter, message);
            }
        }

        /// <summary>
        /// Dispose the logger.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Write a messages without returning to new line.
        /// </summary>
        internal static void Write(OutputWriter outputWriter, string message, ConsoleColor? background = default, ConsoleColor? foreground = default)
        {
            outputWriter.SetColor(background, foreground);
            outputWriter.Write(message);
            outputWriter.ResetColor();
        }

        /// <summary>
        /// Write a messages and returns to new line.
        /// </summary>
        internal static void WriteLine(OutputWriter outputWriter, string message, ConsoleColor? background = default, ConsoleColor? foreground = default)
        {
            outputWriter.SetColor(background, foreground);
            outputWriter.WriteLine(message);
            outputWriter.ResetColor();
        }

        /// <summary>
        /// Get a log message from a value and an event id.
        /// </summary>
        internal static (string Message, object[] Args) GetLogMessageFrom<T>(T value, EventId id)
        {
            var typeofT = typeof(T);

            var sb = new StringBuilder();

            List<PropertyInfo> membersInfos;

            if (!MembersInfo.TryGetValue(typeofT, out membersInfos))
            {
                membersInfos = GetProperties(typeof(T));
            }

            var args = new List<object>(membersInfos.Count + 1);

            // Add event name
            sb.Append($@"Event={{Event}} ");
            args.Add(id.Name);

            for (var i = 0; i < membersInfos.Count; i++)
            {
                var member = membersInfos[i];
                object memberValue = GetValue(member, value);
                sb.Append(CultureInfo.InvariantCulture, $@"{member.Name}={{{member.Name}}} ");
                args.Add(memberValue);
            }

            if (!IsAnonymousType(typeofT))
            {
                sb.Append($@"Type={{Type}} ");
                args.Add(typeofT.Name);
            }

            MembersInfo.TryAdd(typeofT, membersInfos);

            return (sb.ToString(), args.ToArray());
        }

        /// <summary>
        /// Dispose the logger.
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {
            // Dispose managed resources
            if (cleanup)
                this.OutputWriters?.Clear();

            // Dispose unmanaged resources
        }

        private static readonly ConcurrentDictionary<Type, List<PropertyInfo>> MembersInfo = new();

        private static object GetValue(MemberInfo member, object obj)
        {
            var serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

            PropertyInfo pi = member as PropertyInfo;
            return pi != null
                ? pi.PropertyType != null && pi.PropertyType == typeof(DbConnection)
                    ? ((DbConnection)pi.GetValue(obj)).ToLogString()
                    : pi.PropertyType != null && pi.PropertyType == typeof(DbTransaction)
                        ? ((DbTransaction)pi.GetValue(obj)).ToLogString()
                        : pi.PropertyType != null && pi.PropertyType == typeof(DbCommand)
                                        ? ((DbCommand)pi.GetValue(obj)).ToLogString()
                                        : pi.PropertyType != null && pi.PropertyType == typeof(SyncContext)
                                        ? serializer.Serialize((SyncContext)pi.GetValue(obj)).ToUtf8String()
                                        : pi.GetValue(obj)
                : null;
        }

        private static List<PropertyInfo> GetProperties(Type type, BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static)
        {
            List<PropertyInfo> targetMembers = [.. type.GetProperties(flags)];

            List<PropertyInfo> distinctMembers = new List<PropertyInfo>(targetMembers.Count);

            foreach (IGrouping<string, PropertyInfo> groupedMember in targetMembers.GroupBy(m => m.Name))
            {
                int count = groupedMember.Count();
                var member = groupedMember.FirstOrDefault();

                if (member == null)
                    continue;

                distinctMembers.Add(member);
            }

            return [.. distinctMembers.Where(t => !IsIndexedProperty(t)).OrderBy(m => m.Name + m.DeclaringType.Name)];
        }

        private static bool IsIndexedProperty(PropertyInfo propertyInfo) => propertyInfo != null && propertyInfo.GetIndexParameters().Length > 0;

        private static bool IsAnonymousType(Type type) =>
#if NET6_0_OR_GREATER
            Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                       && type.IsGenericType && type.Name.Contains("AnonymousType", SyncGlobalization.DataSourceStringComparison)
                       && (type.Name.StartsWith("<>", SyncGlobalization.DataSourceStringComparison) || type.Name.StartsWith("VB$", SyncGlobalization.DataSourceStringComparison))
                       && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
#else
            Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                       && type.IsGenericType && type.Name.Contains("AnonymousType")
                       && (type.Name.StartsWith("<>", SyncGlobalization.DataSourceStringComparison) || type.Name.StartsWith("VB$", SyncGlobalization.DataSourceStringComparison))
                       && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;

#endif

        private static ConsoleColors GetLogLevelConsoleColors(LogLevel logLevel) =>

            // We must explicitly set the background color if we are setting the foreground color,
            // since just setting one can look bad on the users console.
            logLevel switch
            {
                LogLevel.Critical => new ConsoleColors(ConsoleColor.White, ConsoleColor.Red),
                LogLevel.Error => new ConsoleColors(ConsoleColor.White, ConsoleColor.Red),
                LogLevel.Warning => new ConsoleColors(ConsoleColor.Yellow, ConsoleColor.Black),
                LogLevel.Information => new ConsoleColors(ConsoleColor.DarkGreen, ConsoleColor.Black),
                LogLevel.Debug => new ConsoleColors(ConsoleColor.Yellow, ConsoleColor.Black),
                LogLevel.Trace => new ConsoleColors(ConsoleColor.Gray, ConsoleColor.Black),
                _ => new ConsoleColors(null, null),
            };

        private static string GetLogLevelString(LogLevel logLevel) => logLevel switch
        {
            LogLevel.Trace => "TRC",
            LogLevel.Debug => "DBG",
            LogLevel.Information => "INF",
            LogLevel.Warning => "WRN",
            LogLevel.Error => "ERR",
            LogLevel.Critical => "CRI",
            LogLevel.None => string.Empty,
            _ => throw new ArgumentOutOfRangeException(nameof(logLevel)),
        };

        private readonly struct ConsoleColors
        {
            public ConsoleColors(ConsoleColor? foreground, ConsoleColor? background)
            {
                this.Foreground = foreground;
                this.Background = background;
            }

            public ConsoleColor? Foreground { get; }

            public ConsoleColor? Background { get; }
        }
    }

    /// <summary>
    /// Output writer to write to console or debug output window.
    /// </summary>
    internal abstract class OutputWriter
    {
        /// <summary>
        /// Write a message to the output writer.
        /// </summary>
        internal abstract void Write(string value);

        /// <summary>
        /// Write a formatted message to the output writer.
        /// </summary>
        internal abstract void Write(string format, params object[] arg);

        /// <summary>
        /// Write a message to the output writer and return to a new line.
        /// </summary>
        internal abstract void WriteLine(string value);

        /// <summary>
        /// Write a formatted message to the output writer and return to a new line.
        /// </summary>
        internal abstract void WriteLine(string format, params object[] arg);

        /// <summary>
        /// Gets the name of the output writer.
        /// </summary>
        internal abstract string Name { get; }

        /// <summary>
        /// Gets a value indicating whether the output writer supports color.
        /// </summary>
        internal abstract bool SupportsColor { get; }

        /// <summary>
        /// Reset the color of the output writer.
        /// </summary>
        internal abstract void ResetColor();

        /// <summary>
        /// Set the color of the output writer.
        /// </summary>
        internal abstract bool SetColor(ConsoleColor? background, ConsoleColor? foreground);
    }

    /// <summary>
    /// Console output writer to write to console.
    /// </summary>
    internal class ConsoleWriter : OutputWriter
    {

        /// <inheritdoc/>
        internal override bool SupportsColor => true;

        /// <inheritdoc/>
        internal override string Name => "Console";

        /// <inheritdoc/>
        internal override void Write(string value) => Console.Write(value);

        /// <inheritdoc/>
        internal override void Write(string format, params object[] arg) => Console.Write(format, arg);

        /// <inheritdoc/>
        internal override void WriteLine(string value) => Console.WriteLine(value);

        /// <inheritdoc/>
        internal override void WriteLine(string format, params object[] arg) => Console.WriteLine(format, arg);

        /// <inheritdoc/>
        internal override void ResetColor() => Console.ResetColor();

        /// <inheritdoc/>
        internal override bool SetColor(ConsoleColor? background, ConsoleColor? foreground)
        {
            if (background.HasValue)
                Console.BackgroundColor = background.Value;

            if (foreground.HasValue)
                Console.ForegroundColor = foreground.Value;

            return background.HasValue || foreground.HasValue;
        }
    }

    /// <summary>
    /// Debug output writer to write to debug output window.
    /// </summary>
    internal class DebugWriter : OutputWriter
    {
        /// <inheritdoc/>
        internal override bool SupportsColor => false;

        /// <inheritdoc/>
        internal override string Name => "Debug";

        /// <inheritdoc/>
        internal override void Write(string value) => Debug.Write(value);

        /// <inheritdoc/>
        internal override void Write(string format, params object[] arg) => Debug.Write(string.Format(CultureInfo.InvariantCulture, format, arg));

        /// <inheritdoc/>
        internal override void WriteLine(string value) => Debug.WriteLine(value);

        /// <inheritdoc/>
        internal override void WriteLine(string format, params object[] arg) => Debug.WriteLine(format, arg);

        /// <inheritdoc/>
        internal override void ResetColor() { }

        /// <inheritdoc/>
        internal override bool SetColor(ConsoleColor? background, ConsoleColor? foreground) => false;
    }
}