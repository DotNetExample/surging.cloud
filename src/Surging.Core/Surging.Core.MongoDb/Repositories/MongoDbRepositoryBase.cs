using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Surging.Core.CPlatform.Exceptions;
using Surging.Core.Domain.Entities;
using Surging.Core.MongoDb.Provider;
using System.Linq;

namespace Surging.Core.MongoDb.Repositories
{


    public class MongoDbRepositoryBase<TEntity> : MongoDbRepositoryBase<TEntity, int>
        where TEntity : class, IEntity<int>
    {
        public MongoDbRepositoryBase(IMongoDatabaseProvider databaseProvider)
            : base(databaseProvider)
        {
        }
    }

   
    public class MongoDbRepositoryBase<TEntity, TPrimaryKey> : IMongoDRepository<TEntity, TPrimaryKey>
        where TEntity : class, IEntity<TPrimaryKey>
    {
        public virtual MongoDatabase Database
        {
            get { return _databaseProvider.GetDatabase(); }
        }

        public virtual MongoCollection<TEntity> Collection
        {
            get
            {
                return _databaseProvider.GetDatabase().GetCollection<TEntity>(typeof(TEntity).Name);
            }
        }

        private readonly IMongoDatabaseProvider _databaseProvider;

        public MongoDbRepositoryBase(IMongoDatabaseProvider databaseProvider)
        {
            _databaseProvider = databaseProvider;
        }

        public virtual IQueryable<TEntity> GetAll()
        {
            return Collection.AsQueryable();
        }

        public virtual TEntity Get(TPrimaryKey id)
        {
            var query = MongoDB.Driver.Builders.Query<TEntity>.EQ(e => e.Id, id);
            var entity = Collection.FindOne(query);
            if (entity == null)
            {
                throw new DataAccessException("There is no such an entity with given primary key. Entity type: " + typeof(TEntity).FullName + ", primary key: " + id);
            }

            return entity;
        }

        public virtual TEntity FirstOrDefault(TPrimaryKey id)
        {
            var query = MongoDB.Driver.Builders.Query<TEntity>.EQ(e => e.Id, id);
            return Collection.FindOne(query);
        }

        public virtual TEntity Insert(TEntity entity)
        {
            Collection.Insert(entity);
            return entity;
        }
        public virtual TEntity Update(TEntity entity)
        {
            Collection.Save(entity);
            return entity;
        }

        public virtual void Delete(TEntity entity)
        {
            Delete(entity.Id);
        }

        public virtual void Delete(TPrimaryKey id)
        {
            var query = MongoDB.Driver.Builders.Query<TEntity>.EQ(e => e.Id, id);
            Collection.Remove(query);
        }
    }
}
