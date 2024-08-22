using McMaster.Extensions.CommandLineUtils;
using System;

namespace WebSyncClient
{
    [Subcommand(typeof(SyncOldCommand))]
    [Subcommand(typeof(SyncLastCommand))]
    public class Board
    {
        public Board()
        {
        }

        protected static int OnExecute(CommandLineApplication app)
        {
            // this shows help even if the --help option isn't specified
            app.ShowHelp();
            Console.WriteLine("Show Board");
            return 1;
        }
    }
}