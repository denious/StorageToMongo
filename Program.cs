using System;
using Microsoft.Extensions.DependencyInjection;
using Serilog;

namespace StorageToMongo
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // initialize program
                var startup = new Startup();

                // prepare migration
                var migration = startup.ServiceCollection.GetRequiredService<Migration>();

                // test run
                // TODO: grab table names from args
                var task = migration.Run();
                var awaiter = task.GetAwaiter();
                awaiter.GetResult();
            }
            catch (Exception e)
            {
                Log.Error(e,"Exception in Main");
            }

            Console.WriteLine();
            Console.Write("Press any key to exit");
            Console.ReadKey();
        }
    }
}
