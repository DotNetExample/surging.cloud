using org.apache.zookeeper;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Surging.Core.Zookeeper.WatcherProvider
{
   public abstract class WatcherBase
    {
        protected string Path { get; }

        protected WatcherBase(string path)
        {
            Path = path;
        }

    }
}
