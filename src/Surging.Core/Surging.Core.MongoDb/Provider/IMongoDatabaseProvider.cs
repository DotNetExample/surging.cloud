using MongoDB.Driver;
using Surging.Core.CPlatform.Ioc;

namespace Surging.Core.MongoDb.Provider
{
    public interface IMongoDatabaseProvider: ITransientDependency
    {

        MongoDatabase GetDatabase();
    }
}
