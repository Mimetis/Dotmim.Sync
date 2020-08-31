using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Dotmim.Sync
{

    /// <summary>
    /// Default logger used in Dotmim.Sync
    /// </summary>
    public class SyncLogger : ILogger, IDisposable
    {
        internal List<OutputWriter> outputWriters = new List<OutputWriter>();

        /// <summary>
        /// Gets a value indicating the mimimum LogLevel value
        /// </summary>
        public LogLevel MinimumLevel { get; internal set; }

        public SyncLogger() => this.MinimumLevel = LogLevel.Error;

        /// <summary>
        /// Adds an output to console when logging something
        /// </summary>
        public SyncLogger AddConsole()
        {
            if (!outputWriters.Any(w => w.Name == "Console"))
                outputWriters.Add(new ConsoleWriter());

            return this;
        }

        /// <summary>
        /// Adds an output to diagnostics debug window when logging something
        /// </summary>
        public SyncLogger AddDebug()
        {
            if (!outputWriters.Any(w => w.Name == "Debug"))
                outputWriters.Add(new DebugWriter());

            return this;
        }

        /// <summary>
        /// Adds minimum level : 0 Trace, 1 Debug, 2 Information, 3, Warning, 4 Error, 5 Critical, 6 None
        /// </summary>
        public SyncLogger SetMinimumLevel(LogLevel minimumLevel)
        {
            this.MinimumLevel = minimumLevel;
            return this;
        }


        public IDisposable BeginScope<TState>(TState state) => this;

        public void Dispose() { }


        /// <summary>
        /// Gets if the logger can log something, according to the minimum log level parameterized
        /// </summary>
        public bool IsEnabled(LogLevel logLevel) => this.MinimumLevel <= logLevel;

        /// <summary>
        /// Log to all output writers configured
        /// </summary>
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;

            if (this.outputWriters.Count <= 0)
                return;

            var now = DateTime.UtcNow.TimeOfDay.ToString(@"hh\:mm\:ss\.ff");

            var message = formatter(state, exception) ?? string.Empty;
            var levelColors = GetLogLevelConsoleColors(logLevel);

            foreach (var outputWriter in this.outputWriters)
            {
                Write(outputWriter, "[", ConsoleColor.Black, ConsoleColor.DarkGray);
                Write(outputWriter, now, ConsoleColor.Black, ConsoleColor.White);
                Write(outputWriter, "]", ConsoleColor.Black, ConsoleColor.DarkGray);
                Write(outputWriter, " ");

                Write(outputWriter, $"{GetLogLevelString(logLevel)}", levelColors.Background, levelColors.Foreground);
                Write(outputWriter, ": ");
                if (eventId != null)
                {
                    Write(outputWriter, "[", ConsoleColor.Black, ConsoleColor.DarkGray);
                    Write(outputWriter, string.IsNullOrEmpty(eventId.Name) ? eventId.Id.ToString() : eventId.Name, ConsoleColor.Black, ConsoleColor.White);
                    Write(outputWriter, "]", ConsoleColor.Black, ConsoleColor.DarkGray);
                    Write(outputWriter, " ");
                }
                WriteLine(outputWriter, message);
            }
        }

        /// <summary>
        /// Write a messages without returning to new line
        /// </summary>
        internal void Write(OutputWriter outputWriter, string message, ConsoleColor? background = default, ConsoleColor? foreground = default)
        {
            outputWriter.SetColor(background, foreground);
            outputWriter.Write(message);
            outputWriter.ResetColor();
        }

        /// <summary>
        /// Write a messages and returns to new line
        /// </summary>
        internal void WriteLine(OutputWriter outputWriter, string message, ConsoleColor? background = default, ConsoleColor? foreground = default)
        {
            outputWriter.SetColor(background, foreground);
            outputWriter.WriteLine(message);
            outputWriter.ResetColor();
        }


        private static Dictionary<Type, List<PropertyInfo>> MembersInfo = new Dictionary<Type, List<PropertyInfo>>();

        //internal void Serialize(object obj, Type objType)
        //{

        //    // If it's Object, we get the underlying type
        //    if (objType == typeof(Object) && obj != null)
        //    {
        //        var baseType = obj.GetType().GetTypeInfo();
        //        // var s = TypeSerializer.GetSerializer(baseType);
        //        Serialize(obj, baseType);
        //        return;
        //    }
        //}

        //public static bool IsEnumerable(Type type)
        //{
        //    if (type.IsArray)
        //        return true;

        //    if (typeof(IEnumerable).IsAssignableFrom(type))
        //        return true;

        //    return false;
        //}

        //public static bool IsPrimitiveManagedType(Type valueType)
        //{
        //    if (valueType == typeof(bool))
        //        return true;
        //    else if (valueType == typeof(byte))
        //        return true;
        //    else if (valueType == typeof(char))
        //        return true;
        //    else if (valueType == typeof(double))
        //        return true;
        //    else if (valueType == typeof(float))
        //        return true;
        //    else if (valueType == typeof(int))
        //        return true;
        //    else if (valueType == typeof(long))
        //        return true;
        //    else if (valueType == typeof(short))
        //        return true;
        //    else if (valueType == typeof(uint))
        //        return true;
        //    else if (valueType == typeof(ulong))
        //        return true;
        //    else if (valueType == typeof(ushort))
        //        return true;
        //    else if (valueType == typeof(byte[]))
        //        return true;
        //    else if (valueType == typeof(DateTime))
        //        return true;
        //    else if (valueType == typeof(DateTimeOffset))
        //        return true;
        //    else if (valueType == typeof(Decimal))
        //        return true;
        //    else if (valueType == typeof(Guid))
        //        return true;
        //    else if (valueType == typeof(String))
        //        return true;
        //    else if (valueType == typeof(SByte))
        //        return true;
        //    else if (valueType == typeof(TimeSpan))
        //        return true;

        //    return false;
        //}

        //internal static void Recursive<T>(T value, StringBuilder sb, List<object> args)
        //{
        //    var typeofT = value.GetType();

        //    List<PropertyInfo> membersInfos;
        //    if (MembersInfo.ContainsKey(typeofT))
        //    {
        //        membersInfos = MembersInfo[typeofT];

        //    }
        //    else
        //    {
        //        membersInfos = GetProperties(typeofT);

        //        if (!MembersInfo.ContainsKey(typeofT))
        //            lock (MembersInfo)
        //                if (!MembersInfo.ContainsKey(typeofT))
        //                    MembersInfo.Add(typeofT, membersInfos);
        //    }



        //    for (var i = 0; i < membersInfos.Count; i++)
        //    {
        //        var member = membersInfos[i];
        //        var memberBaseType = member.PropertyType;
        //        object memberValue = GetValue(member, value);

        //        var isDb = memberBaseType == typeof(IDbConnection) || memberBaseType == typeof(IDbTransaction) || memberBaseType == typeof(IDbCommand);

        //        if (!isDb && !IsPrimitiveManagedType(memberBaseType) && !IsEnumerable(memberBaseType) && memberValue != null)
        //        {
        //            Recursive(memberValue, sb, args);
        //        }
        //        else
        //        {

        //            sb.Append($@"{member.Name}={{{member.Name}}} ");
        //            args.Add(memberValue);
        //        }
        //    }

        //}

        internal static (string Message, object[] Args) GetLogMessageFrom<T>(T value, EventId id)
        {
            var typeofT = typeof(T);

            var sb = new StringBuilder();

            List<PropertyInfo> membersInfos;

            if (MembersInfo.ContainsKey(typeofT))
                membersInfos = MembersInfo[typeofT];
            else
                membersInfos = GetProperties(typeof(T));

            var args = new List<object>(membersInfos.Count + 1);

            // Add event name            
            sb.Append($@"Event={{Event}} ");
            args.Add(id.Name);

            for (var i = 0; i < membersInfos.Count; i++)
            {
                var member = membersInfos[i];
                object memberValue = GetValue(member, value);
                sb.Append($@"{member.Name}={{{member.Name}}} ");
                args.Add(memberValue);
            }

            if (!IsAnonymousType(typeofT))
            {
                sb.Append($@"Type={{Type}} ");
                args.Add(typeofT.Name);
            }

            if (!MembersInfo.ContainsKey(typeofT))
                lock (MembersInfo)
                    if (!MembersInfo.ContainsKey(typeofT))
                        MembersInfo.Add(typeofT, membersInfos);

            return (sb.ToString(), args.ToArray());
        }


        private static object GetValue(MemberInfo member, object obj)
        {

            PropertyInfo pi = member as PropertyInfo;
            if (pi != null)
            {
                if (pi.PropertyType != null && pi.PropertyType == typeof(DbConnection))
                    return ((DbConnection)pi.GetValue(obj)).ToLogString();
                else if (pi.PropertyType != null && pi.PropertyType == typeof(DbTransaction))
                    return ((DbTransaction)pi.GetValue(obj)).ToLogString();
                else
                    return pi.GetValue(obj);
            }

            return null;

        }

        private static List<PropertyInfo> GetProperties(Type type, BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static)
        {
            List<PropertyInfo> targetMembers = new List<PropertyInfo>();

            targetMembers.AddRange(type.GetProperties(flags));

            List<PropertyInfo> distinctMembers = new List<PropertyInfo>(targetMembers.Count);

            foreach (IGrouping<string, PropertyInfo> groupedMember in targetMembers.GroupBy(m => m.Name))
            {
                int count = groupedMember.Count();
                var member = groupedMember.FirstOrDefault();

                if (member == null)
                    continue;

                distinctMembers.Add(member);
            }

            return distinctMembers.Where(t => !IsIndexedProperty(t)).OrderBy(m => m.Name + m.DeclaringType.Name).ToList();
        }

        private static bool IsIndexedProperty(PropertyInfo propertyInfo)
        {
            return propertyInfo != null ? propertyInfo.GetIndexParameters().Length > 0 : false;
        }


        private static bool IsAnonymousType(Type type)
        {
            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                       && type.IsGenericType && type.Name.Contains("AnonymousType")
                       && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
                       && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }

        private ConsoleColors GetLogLevelConsoleColors(LogLevel logLevel)
        {
            // We must explicitly set the background color if we are setting the foreground color,
            // since just setting one can look bad on the users console.
            switch (logLevel)
            {
                case LogLevel.Critical:
                    return new ConsoleColors(ConsoleColor.White, ConsoleColor.Red);
                case LogLevel.Error:
                    return new ConsoleColors(ConsoleColor.White, ConsoleColor.Red);
                case LogLevel.Warning:
                    return new ConsoleColors(ConsoleColor.Yellow, ConsoleColor.Black);
                case LogLevel.Information:
                    return new ConsoleColors(ConsoleColor.DarkGreen, ConsoleColor.Black);
                case LogLevel.Debug:
                    return new ConsoleColors(ConsoleColor.Yellow, ConsoleColor.Black);
                case LogLevel.Trace:
                    return new ConsoleColors(ConsoleColor.Gray, ConsoleColor.Black);
                default:
                    return new ConsoleColors(null, null);
            }
        }

        private static string GetLogLevelString(LogLevel logLevel)
        {
            switch (logLevel)
            {
                case LogLevel.Trace:
                    return "TRC";
                case LogLevel.Debug:
                    return "DBG";
                case LogLevel.Information:
                    return "INF";
                case LogLevel.Warning:
                    return "WRN";
                case LogLevel.Error:
                    return "ERR";
                case LogLevel.Critical:
                    return "CRI";
                default:
                    throw new ArgumentOutOfRangeException(nameof(logLevel));
            }
        }

        private readonly struct ConsoleColors
        {
            public ConsoleColors(ConsoleColor? foreground, ConsoleColor? background)
            {
                Foreground = foreground;
                Background = background;
            }

            public ConsoleColor? Foreground { get; }

            public ConsoleColor? Background { get; }
        }


    }


    internal abstract class OutputWriter
    {
        internal abstract void Write(string value);
        internal abstract void Write(string format, params object[] arg);
        internal abstract void WriteLine(string value);
        internal abstract void WriteLine(string format, params object[] arg);
        internal abstract string Name { get; }

        internal abstract bool SupportsColor { get; }
        internal abstract void ResetColor();

        internal abstract bool SetColor(ConsoleColor? background, ConsoleColor? foreground);
    }

    internal class ConsoleWriter : OutputWriter
    {
        internal override bool SupportsColor => true;

        internal override string Name => "Console";

        internal override void Write(string value) => Console.Write(value);
        internal override void Write(string format, params object[] arg) => Console.Write(format, arg);
        internal override void WriteLine(string value) => Console.WriteLine(value);
        internal override void WriteLine(string format, params object[] arg) => Console.WriteLine(format, arg);
        internal override void ResetColor() => Console.ResetColor();
        internal override bool SetColor(ConsoleColor? background, ConsoleColor? foreground)
        {
            if (background.HasValue)
                Console.BackgroundColor = background.Value;

            if (foreground.HasValue)
                Console.ForegroundColor = foreground.Value;

            return background.HasValue || foreground.HasValue;
        }

    }
    internal class DebugWriter : OutputWriter
    {
        internal override bool SupportsColor => false;
        internal override string Name => "Debug";
        internal override void Write(string value) => Debug.Write(value);
        internal override void Write(string format, params object[] arg) => Debug.Write(string.Format(format, arg));
        internal override void WriteLine(string value) => Debug.WriteLine(value);
        internal override void WriteLine(string format, params object[] arg) => Debug.WriteLine(format, arg);
        internal override void ResetColor() { }
        internal override bool SetColor(ConsoleColor? background, ConsoleColor? foreground) => false;


    }
}
