using System;
using System.Linq;

namespace Dotmim.Sync.Tools
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // From dotnet command line or debug mode
                if (args.Length == 0)
                    //args = new string[] { "p0", "provider", "-s", "client", "-p", "sqlite", "-c", "adv.db" };
                    args = new string[] {  "p0", "-s" };
                    
                Runner.Execute(args);
            }
            catch (Exception ex)
            {
                var d = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ForegroundColor = d;
            }
        }
    }
}
