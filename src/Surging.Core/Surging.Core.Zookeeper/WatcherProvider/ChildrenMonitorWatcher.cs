using org.apache.zookeeper;
using Rabbit.Zookeeper;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Surging.Core.Zookeeper.WatcherProvider
{
    internal class ChildrenMonitorWatcher : WatcherBase
    {
        private Action<string[], string[]> _action;
        private string[] _currentData;


        public ChildrenMonitorWatcher(string path, Action<string[], string[]> action) : base(path)
        {
            _action = action;
            _currentData = new string[0];
        }

        internal void SetCurrentData(string[] currentData)
        {
            _currentData = currentData ?? new string[0];
        }

        internal async Task HandleChildrenChange(IZookeeperClient client, NodeChildrenChangeArgs args)
        {
            Watcher.Event.EventType eventType = args.Type;
            var path = args.Path;
            var watcher = new ChildrenMonitorWatcher(path, _action);
            switch (eventType)
            {
                case Watcher.Event.EventType.NodeCreated:
                    await client.SubscribeChildrenChange(path, watcher.HandleChildrenChange);
                    break;
                case Watcher.Event.EventType.NodeDataChanged:
                    try
                    {

                        var currentChildrens = new string[0];
                        if (args.CurrentChildrens != null && args.CurrentChildrens.Any())
                        {
                            currentChildrens = args.CurrentChildrens.ToArray();
                        }
                        _action(_currentData, currentChildrens);
                        watcher.SetCurrentData(currentChildrens);

                    }
                    catch (KeeperException.NoNodeException)
                    {
                        _action(_currentData, new string[0]);
                        watcher.SetCurrentData(new string[0]);
                    }
                    break;
                case Watcher.Event.EventType.NodeDeleted:
                    _action(_currentData, new string[0]);
                    watcher.SetCurrentData(new string[0]);
                    break;

            }
        }


    }
}
