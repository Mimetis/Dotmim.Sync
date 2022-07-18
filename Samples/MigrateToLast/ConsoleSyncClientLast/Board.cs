using System;
using System.Collections.Generic;
using System.Text;
using McMaster.Extensions.CommandLineUtils;

namespace WebSyncClient
{
    [Subcommand(typeof(SyncOldCommand))]
    [Subcommand(typeof(SyncLastCommand))]
    public class Board
    {
        public Board()
        {
        }

        protected int OnExecute(CommandLineApplication app)
        {
            // this shows help even if the --help option isn't specified
            app.ShowHelp();
            Console.WriteLine("Show Board");
            return 1;
        }

    }
}
