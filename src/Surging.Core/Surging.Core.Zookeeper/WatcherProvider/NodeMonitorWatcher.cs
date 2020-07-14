using org.apache.zookeeper;
using Rabbit.Zookeeper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Surging.Core.Zookeeper.WatcherProvider
{
    internal class NodeMonitorWatcher : WatcherBase
    {
        private readonly Action<byte[], byte[]> _action;
        private byte[] _currentData;


        public NodeMonitorWatcher(string path, Action<byte[], byte[]> action) : base (path)
        {
            _action = action;
            _currentData = new byte[0];
        }

        //public void SetCurrentData(byte[] currentData)
        //{
        //    _currentData = currentData;
        //}

        internal async Task HandleNodeDataChange(IZookeeperClient client, NodeDataChangeArgs args)
        {
            Watcher.Event.EventType eventType = args.Type;
            var nodeData = new byte[0];
            if (args.CurrentData != null && args.CurrentData.Any())
            {
                nodeData = args.CurrentData.ToArray();
            }
            switch (eventType)
            {
                case Watcher.Event.EventType.NodeCreated:                    
                    _action(new byte[0], nodeData);
                    _currentData = nodeData;
                    break;

                case Watcher.Event.EventType.NodeDataChanged:
                    _action(_currentData, nodeData);
                    _currentData = nodeData;
                    break;
            }
       
        }

    }
}