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
                    //args = new string[] { "table", "northwind", "-a", "Customers" };
                    args = new string[] {  "-s", "adv" };
                    
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
