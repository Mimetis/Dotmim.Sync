//using Dotmim.Sync.Enumerations;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Text;

//namespace Dotmim.Sync
//{
//    /// <summary>
//    /// Generate a SyncAgent with correct config on each provider
//    /// </summary>
//    public class SyncAgentBuilder
//    {
//        internal SyncAgent syncAgent;
//        internal SyncAgentBuilder(SyncAgent agent)
//        {
//            this.syncAgent = agent;
//        }

//        /// <summary>
//        ///  Check if everything is in place
//        /// </summary>
//        public SyncAgent Build()
//        {
//            // TODO : Check every param and log everything if needed
//            // Check providers are correct
//            // Check we have two providers , one local one server
//            // Check if server provider supports to be a server provider
//            // Check if we have the connection string
//            // and so on ....
//            return this.syncAgent;
//        }

//        // Add a core provider
//        internal SyncCoreProviderBuilder AddCoreProvider(CoreProvider coreProvider) 
//            => new SyncCoreProviderBuilder(this, coreProvider);
//    }

//    /// <summary>
//    /// Define what kind of provider we are configuring.
//    /// </summary>
//    public class SyncCoreProviderBuilder
//    {
//        private SyncAgentBuilder syncAgentBuilder;

//        public SyncCoreProviderBuilder(SyncAgentBuilder syncAgentBuilder, CoreProvider provider)
//        {
//            this.syncAgentBuilder = syncAgentBuilder;
//            this.provider = provider;
//        }

//        private CoreProvider provider;

//        public SyncAgentBuilder AsServer(Action<SyncConfiguration> options)
//        {
//            // set configuration 
//            options(this.syncAgentBuilder.syncAgent.Configuration);

//            // affect this provider as the remote provider
//            this.syncAgentBuilder.syncAgent.RemoteProvider = this.provider;

//            return this.syncAgentBuilder;
//        }

//        public SyncAgentBuilder AsClient()
//        {
//            // affect this provider as the remote provider
//            this.syncAgentBuilder.syncAgent.RemoteProvider = this.provider;

//            return this.syncAgentBuilder;
//        }
//    }

//    internal static class SyncAgentBuilderDependencyInjection
//    {
//        public static SyncCoreProviderBuilder UseSqlServer(this SyncAgentBuilder builder, Action<SyncOptions> SetOptions)
//        {
//            // Create a SQL server provider in the correct assembly
//            // SqlServerProvider coreProvider = new SqlServerProvider(connectionString);
//            CoreProvider coreProvider = null;

//            SetOptions(coreProvider.Options);
//            return builder.AddCoreProvider(coreProvider);
//        }

//        public static SyncCoreProviderBuilder UseSqlServer(this SyncAgentBuilder builder, String connectionString)
//        {
//            // Create a SQL server provider in the correct assembly
//            // SqlServerProvider coreProvider = new SqlServerProvider(connectionString);
//            CoreProvider coreProvider = null;

//            return builder.AddCoreProvider(coreProvider);
//        }
//    }


//    public static class SyncBuilder
//    {
//        public static SyncAgentBuilder CreateSyncAgentBuilder(string scopeName = null)
//        {
//            return new SyncAgentBuilder(new SyncAgent(scopeName, null, null);
//        }

//        public static void totot()
//        {
//            var agent = SyncBuilder.CreateSyncAgentBuilder()
//                .UseSqlServer("Data Source....").AsServer(conf => conf.SerializationFormat = SerializationFormat.Json)
//                .UseSqlServer(opt => opt.BatchDirectory = Directory.GetCurrentDirectory()).AsClient()
//                .Build();
//        }
//    }
//}
