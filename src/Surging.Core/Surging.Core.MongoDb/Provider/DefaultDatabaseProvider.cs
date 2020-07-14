using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace Surging.Core.MongoDb.Provider
{
    public class DefaultDatabaseProvider : IMongoDatabaseProvider
    {

        public MongoDatabase GetDatabase()
        {
            var client = new MongoClient(AppConfig.MongoDbOption.ConnectionString)
               .GetServer()
               .GetDatabase(AppConfig.MongoDbOption.DatabaseName);
            return client;
        }
    }
}
