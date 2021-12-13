using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{
    //
    // Summary:
    //     Defines logging severity levels.
    public enum SyncProgressLevel
    {
        //
        // Summary:
        //     Progress that contain the most detailed messages and the Sql statement executed
        //     These messages may contain sensitive
        //     application data. These messages are disabled by default and should never be
        //     enabled in a production environment.
        Sql,
        //
        // Summary:
        //     Progress that contain the most detailed messages. These messages may contain sensitive
        //     application data. These messages are disabled by default and should never be
        //     enabled in a production environment.
        Trace,
        //
        // Summary:
        //     Progress that are used for interactive investigation during development. These logs
        //     should primarily contain information useful for debugging and have no long-term
        //     value.
        Debug,
        //
        // Summary:
        //     Progress that track the general flow of the application. These logs should have long-term
        //     value.
        Information,
        //
        // Summary:
        //     Progress that highlight an abnormal or unexpected event in the application flow,
        //     but do not otherwise cause the application execution to stop.
        Warning,
        //
        // Summary:
        //     Progress that highlight when the current flow of execution is stopped due to a failure.
        //     These should indicate a failure in the current activity, not an application-wide
        //     failure.
        Error,
        //
        // Summary:
        //     Not used for writing progress messages. Specifies that a logging category should not
        //     write any messages.
        None
    }
}
