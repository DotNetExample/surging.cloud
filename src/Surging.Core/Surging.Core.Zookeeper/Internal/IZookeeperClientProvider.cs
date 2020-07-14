using Rabbit.Zookeeper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Surging.Core.Zookeeper.Internal
{
    public interface IZookeeperClientProvider : IDisposable
    {
        Task<IZookeeperClient> GetZooKeeperClient();

        Task<IEnumerable<IZookeeperClient>> GetZooKeeperClients();

        Task Check();
    }
}
