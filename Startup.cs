using System.IO;
using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using MongoDB.Driver;
using Serilog;
using Serilog.Events;
using Serilog.Exceptions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace StorageToMongo
{
    public class Startup
    {
        public ServiceProvider ServiceCollection { get; }
        private IConfiguration Configuration { get; }

        public Startup()
        {
            // configure application
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

#if DEBUG
            builder.AddUserSecrets<Startup>();
#endif

            Configuration = builder.Build();

            // configure DI
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);

            ServiceCollection = serviceCollection.BuildServiceProvider();
        }

        private void ConfigureServices(ServiceCollection serviceCollection)
        {
            // add configuration
            serviceCollection.AddSingleton(Configuration);

            // add logging
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.WithExceptionDetails()
                .WriteTo.Debug()
                .WriteTo.Console()
                .WriteTo.File("migration.log", LogEventLevel.Information)
                .CreateLogger();

            serviceCollection
                .AddLogging(builder => builder
                    .AddSerilog()
                    .SetMinimumLevel(LogLevel.Debug));

            // add DB access
            // TODO: switch to config file use
            serviceCollection
                .AddSingleton(provider =>
                {
                    var account = CloudStorageAccount.Parse(Configuration["ConnectionStrings:TableStorage"]);

                    var tableServicePoint = ServicePointManager.FindServicePoint(account.TableEndpoint);
                    tableServicePoint.UseNagleAlgorithm = false;

                    return account.CreateCloudTableClient();
                }).AddSingleton(provider =>
                {
                    var client = new MongoClient(Configuration["ConnectionStrings:Mongo"]);
                    var db = client.GetDatabase(Configuration["MongoDbName"]);
                    return db;
                });

            // add migration service
            serviceCollection.AddSingleton<Migration>();
        }
    }
}
