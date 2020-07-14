using Surging.Core.CPlatform.Convertibles;
using Surging.Core.CPlatform.Exceptions;
using Surging.Core.CPlatform.Messages;
using Surging.Core.CPlatform.Runtime.Client;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Surging.Core.CPlatform.Support.Implementation
{
    public class FailoverHandoverInvoker : IClusterInvoker
    {
        #region Field
        private readonly IRemoteInvokeService _remoteInvokeService;
        private readonly ITypeConvertibleService _typeConvertibleService;
        private readonly IBreakeRemoteInvokeService _breakeRemoteInvokeService;
        private readonly IServiceCommandProvider _commandProvider;
        #endregion Field

        #region Constructor

        public FailoverHandoverInvoker(IRemoteInvokeService remoteInvokeService, IServiceCommandProvider commandProvider,
            ITypeConvertibleService typeConvertibleService, IBreakeRemoteInvokeService breakeRemoteInvokeService)
        {
            _remoteInvokeService = remoteInvokeService;
            _typeConvertibleService = typeConvertibleService;
            _breakeRemoteInvokeService = breakeRemoteInvokeService;
            _commandProvider = commandProvider;
        }

        #endregion Constructor

        public async Task<T> Invoke<T>(IDictionary<string, object> parameters, string serviceId, string _serviceKey, bool decodeJOject)
        {
            var time = 0;
            T result = default(T);
            RemoteInvokeResultMessage message = null;
            var vtCommand = _commandProvider.GetCommand(serviceId);
            var command = vtCommand.IsCompletedSuccessfully ? vtCommand.Result : await vtCommand;
            do
            {
                message = await _breakeRemoteInvokeService.InvokeAsync(parameters, serviceId, _serviceKey, decodeJOject);

                if (message != null)
                {
                    if (message.StatusCode == StatusCode.Success)
                    {
                        if (message.Result != null)
                        {
                            result = (T)_typeConvertibleService.Convert(message.Result, typeof(T));
                        }
                        else
                        {
                            result = default(T);
                        }
                    }
                    else if (message.IsFailedRemoteInvokeCalled())
                    {
                        continue;
                    }
                    else if (message.IsSucceedRemoteInvokeCalled())
                    {
                        throw message.GetExceptionByStatusCode();
                    }

                }
            } while ((message == null || !message.IsSucceedRemoteInvokeCalled()) && ++time < command.FailoverCluster);
            if (message == null)
            {
                throw new CPlatformException($"{serviceId}远程服务调用失败,暂不存在可用的服务实例");
            }
            if (message.StatusCode != StatusCode.Success)
            {
                throw message.GetExceptionByStatusCode();
            }
            return result;
        }

        public async Task Invoke(IDictionary<string, object> parameters, string serviceId, string _serviceKey, bool decodeJOject)
        {
            var time = 0;
            var command = await _commandProvider.GetCommand(serviceId);
            RemoteInvokeResultMessage message = null;
            do
            {
                message = await _breakeRemoteInvokeService.InvokeAsync(parameters, serviceId, _serviceKey, decodeJOject);
                if (message != null)
                {
                    if (message.StatusCode == StatusCode.Success) 
                    {
                        break;
                    }
                    else if (message.IsFailedRemoteInvokeCalled())
                    {
                        continue;
                    }
                    else if (message.IsSucceedRemoteInvokeCalled())
                    {
                        throw message.GetExceptionByStatusCode();
                    }
                }
            }
            while ((message == null || !message.IsSucceedRemoteInvokeCalled()) && ++time < command.FailoverCluster);
            if (message == null)
            {
                throw new CPlatformException($"{serviceId}远程服务调用失败,暂不存在可用的服务实例");
            }
            if (message.StatusCode != StatusCode.Success)
            {
                throw message.GetExceptionByStatusCode();
            }
        }

        public async Task<object> Invoke(IDictionary<string, object> parameters, Type returnType, string serviceId, string _serviceKey, bool decodeJOject)
        {
            var time = 0;
            object result = null;
            RemoteInvokeResultMessage message = null;
            var vtCommand = _commandProvider.GetCommand(serviceId);
            var command = vtCommand.IsCompletedSuccessfully ? vtCommand.Result : await vtCommand;
            do
            {
                message = await _breakeRemoteInvokeService.InvokeAsync(parameters, serviceId, _serviceKey, decodeJOject);

                if (message != null)
                {
                    if (message.StatusCode == StatusCode.Success)
                    {
                        if (message.Result != null)
                        {
                            result = _typeConvertibleService.Convert(message.Result, returnType);
                        }
                        else
                        {
                            result = message.Result;
                        }
                    }
                    else if (message.IsFailedRemoteInvokeCalled())
                    {
                        continue;
                    }
                    else if (message.IsSucceedRemoteInvokeCalled())
                    {
                        throw message.GetExceptionByStatusCode();
                    }

                }
            } while ((message == null || !message.IsSucceedRemoteInvokeCalled()) && ++time < command.FailoverCluster);

            if (message == null)
            {
                throw new CPlatformException($"{serviceId}远程服务调用失败,暂不存在可用的服务实例");
            }
            if (message.StatusCode != StatusCode.Success)
            {
                throw message.GetExceptionByStatusCode();
            }
            return result;
        }
    }

}
