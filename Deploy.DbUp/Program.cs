using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using DbUp;
using System.Configuration;

namespace Deploy.DbUp
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
                .LogToConsole();

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

