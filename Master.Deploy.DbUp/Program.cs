using System;
using System.Reflection;
using DbUp;
using System.Configuration;
using DbUp.Helpers;

namespace Master.Deploy.DbUp
{
    public class Program
    {
        public static int Main(string[] args)
        {
            var connectionString = ConfigurationManager.ConnectionStrings["DeployConnectionString"].ConnectionString;

            foreach (var arg in args)
            {
                if (arg.ToLower().StartsWith("/connectionstring:"))
                {
                    connectionString = arg.Substring("/connectionstring:".Length);
                    continue;
                }
                if (false
                    || arg.ToLower().StartsWith("/?")
                    || arg.ToLower().StartsWith("-?")
                    || arg.ToLower().StartsWith("--?")
                    || arg.ToLower().StartsWith("/help")
                    || arg.ToLower().StartsWith("--help")
                    )
                {
                    var name = typeof(Program).Assembly.GetName().Name;
                    Console.WriteLine();
                    Console.WriteLine($"Usage: {name}.exe [/ConnectionString:\"{{connectionstring}}\"]");
                    Console.WriteLine();
                    return 0;
                }
            }

            Console.WriteLine($"ConnectionString: {connectionString}");


            var builder = DeployChanges.To.SqlDatabase(connectionString)
                .WithScriptsEmbeddedInAssembly(Assembly.GetExecutingAssembly())
                .WithTransactionPerScript()
                .LogToConsole()
                .JournalTo(new NullJournal());

            builder.Configure(c => c.ScriptExecutor.ExecutionTimeoutSeconds = 3600);

            var result = builder.Build().PerformUpgrade();

            if (!result.Successful)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(result.Error);
                Console.ResetColor();
                if (System.Diagnostics.Debugger.IsAttached)
                {
                    Console.WriteLine("Press <Enter> to continue...");
                    Console.ReadLine();
                }
                return -1;
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Success!");
            Console.ResetColor();

            if (System.Diagnostics.Debugger.IsAttached)
            {
                Console.WriteLine("Press <Enter> to continue...");
                Console.ReadLine();
            }

            return 0;
        }
    }
}

