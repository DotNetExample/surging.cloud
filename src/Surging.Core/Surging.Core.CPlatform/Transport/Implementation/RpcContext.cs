using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Surging.Core.CPlatform.Transport.Implementation
{

    public class RpcContext
    {
        private ConcurrentDictionary<string, object> contextParameters;
        private static AsyncLocal<RpcContext> rpcContextThreadLocal = new AsyncLocal<RpcContext>();

        public IDictionary<string, object> GetContextParameters()
        {
            return contextParameters;
        }


        public void SetAttachment(string key, object value)
        {
            contextParameters.AddOrUpdate(key, value, (k, v) => value);
        }

        public object GetAttachment(string key)
        {
            contextParameters.TryGetValue(key, out object result);
            return result;
        }


        public void SetContextParameters(IDictionary<string, object> contextParameters)
        {
            foreach (var item in contextParameters) 
            {
                SetAttachment(item.Key, item.Value);
            }
           
        }

       

        public static RpcContext GetContext()
        {
            var context = rpcContextThreadLocal.Value;

            if (context == null)
            {
                context = new RpcContext();
                rpcContextThreadLocal.Value = context;
            }

            return rpcContextThreadLocal.Value;
        }

        public static void RemoveContext()
        {
            rpcContextThreadLocal.Value = null;
        }

        private RpcContext()
        {
            contextParameters = new ConcurrentDictionary<string, object>();
        }
    }

}