﻿using Microsoft.Extensions.Logging;
using Surging.Core.CPlatform.Address;
using Surging.Core.CPlatform.Exceptions;
using Surging.Core.CPlatform.Routing;
using Surging.Core.CPlatform.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Surging.Core.CPlatform.Runtime.Client.HealthChecks.Implementation
{
    /// <summary>
    /// 默认健康检查服务(每10秒会检查一次服务状态，在构造函数中添加服务管理事件) 
    /// </summary>
    public class DefaultHealthCheckService : IHealthCheckService, IDisposable
    {
        private readonly ConcurrentDictionary<ValueTuple<string, int>, MonitorEntry> _dictionary =
            new ConcurrentDictionary<ValueTuple<string, int>, MonitorEntry>();
        private readonly IServiceRouteManager _serviceRouteManager;
        private readonly int _timeout = AppConfig.ServerOptions.HealthCheckTimeout;
        private readonly Timer _timer;
        private EventHandler<HealthCheckEventArgs> _removed;

        private EventHandler<HealthCheckEventArgs> _changed;
        private readonly ILogger<DefaultHealthCheckService> _logger;

        public event EventHandler<HealthCheckEventArgs> Removed
        {
            add { _removed += value; }
            remove { _removed -= value; }
        }

        public event EventHandler<HealthCheckEventArgs> Changed
        {
            add { _changed += value; }
            remove { _changed -= value; }
        }

        /// <summary>
        /// 默认心跳检查服务(每10秒会检查一次服务状态，在构造函数中添加服务管理事件) 
        /// </summary>
        /// <param name="serviceRouteManager"></param>
        public DefaultHealthCheckService(IServiceRouteManager serviceRouteManager)
        {
            _logger = ServiceLocator.GetService<ILogger<DefaultHealthCheckService>>();
            var timeSpan = TimeSpan.FromSeconds(30);

            _serviceRouteManager = serviceRouteManager;
            //建立计时器
            _timer = new Timer(async s =>
            {
                //检查服务是否可用
                Check(_dictionary.ToArray().Select(i => i.Value), _timeout);
                //移除不可用的服务地址
                RemoveUnhealthyAddress(_dictionary.ToArray().Select(i => i.Value).Where(m => m.UnhealthyTimes >= AppConfig.ServerOptions.UnhealthyTimes));
            }, null, timeSpan, timeSpan);

            //去除监控。
            serviceRouteManager.Removed += (s, e) =>
            {
                Remove(e.Route.Address);
            };
            //重新监控。
            serviceRouteManager.Created += async (s, e) =>
            {
                var keys = e.Route.Address.Select(address =>
                {
                    var ipAddress = address as IpAddressModel;
                    return new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port);
                });
                Check(_dictionary.Where(i => keys.Contains(i.Key)).Select(i => i.Value), _timeout);
            };
            //重新监控。
            serviceRouteManager.Changed += async (s, e) =>
            {
                var keys = e.Route.Address.Select(address =>
                {
                    var ipAddress = address as IpAddressModel;
                    return new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port);
                });
                Check(_dictionary.Where(i => keys.Contains(i.Key)).Select(i => i.Value), _timeout);
                var oldKeys = e.OldRoute.Address.Select(address =>
                {
                    var ipAddress = address as IpAddressModel;
                    return new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port);
                });
                Check(_dictionary.Where(i => oldKeys.Contains(i.Key)).Select(i => i.Value), _timeout);
            };
        }


        #region Implementation of IHealthCheckService

        /// <summary>
        /// 监控一个地址。
        /// </summary>
        /// <param name="address">地址模型。</param>
        /// <returns>一个任务。</returns>
        public void Monitor(AddressModel address)
        {
            var ipAddress = address as IpAddressModel;
            _dictionary.TryGetValue(new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port), out MonitorEntry monitorAddress);
            if (monitorAddress == null)
            {
                _logger.LogDebug($"监控{address.ToString()}服务");
                var isHealth = Check(address, _timeout);
                _dictionary.GetOrAdd(new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port), k => new MonitorEntry(address, isHealth));
            }
        }

        /// <summary>
        /// 判断一个地址是否健康。
        /// </summary>
        /// <param name="address">地址模型。</param>
        /// <returns>健康返回true，否则返回false。</returns>
        public async ValueTask<bool> IsHealth(AddressModel address)
        {
            var ipAddress = address as IpAddressModel;
            MonitorEntry entry;
            var isHealth = !_dictionary.TryGetValue(new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port), out entry) ? Check(address, _timeout) : entry.Health;
            OnChanged(new HealthCheckEventArgs(address, isHealth));
            return isHealth;
        }

        /// <summary>
        /// 标记一个地址为失败的。
        /// </summary>
        /// <param name="address">地址模型。</param>
        /// <returns>一个任务。</returns>
        public Task MarkFailure(AddressModel address)
        {
            return Task.Run(() =>
            {
                var ipAddress = address as IpAddressModel;
                var entry = _dictionary.GetOrAdd(new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port), k => new MonitorEntry(address, false));
                entry.Health = false;
            });
        }

        public async Task MarkServiceRouteUnHealth(string serviceId, AddressModel address)
        {
            var ipAddress = address as IpAddressModel;
            var serviceRoute = await _serviceRouteManager.GetAsync(serviceId);
            if (serviceRoute == null) 
            {
                throw new CPlatformException($"不存在Id为{serviceId}的路由信息");
            }
            var checkedServiceRoute = serviceRoute.Address.SingleOrDefault(p => ((IpAddressModel)p).Ip == ipAddress.Ip && ((IpAddressModel)p).Port == ipAddress.Port);
            if (checkedServiceRoute != null) 
            {
                if (checkedServiceRoute.UnhealthyTimes >= AppConfig.ServerOptions.UnhealthyTimes)
                {
                    serviceRoute.Address.Remove(checkedServiceRoute);
                }
                else 
                {
                    checkedServiceRoute.IsHealth = false;
                    checkedServiceRoute.UnhealthyTimes += 1;
                }              
                await _serviceRouteManager.SetRouteAsync(serviceRoute);
            }
           
        }

        public async Task MarkServiceRouteHealth(string serviceId, AddressModel address)
        {
            var ipAddress = address as IpAddressModel;
            var serviceRoute = await _serviceRouteManager.GetAsync(serviceId);
            if (serviceRoute == null)
            {
                throw new CPlatformException($"不存在Id为{serviceId}的路由信息");
            }
            var checkedServiceRoute = serviceRoute.Address.SingleOrDefault(p => ((IpAddressModel)p).Ip == ipAddress.Ip && ((IpAddressModel)p).Port == ipAddress.Port);
            if (checkedServiceRoute != null)
            {
                if (!checkedServiceRoute.IsHealth) 
                {
                    checkedServiceRoute.IsHealth = true;
                    checkedServiceRoute.UnhealthyTimes = 0;
                    await _serviceRouteManager.SetRouteAsync(serviceRoute);
                }
            }
        }

        protected void OnRemoved(params HealthCheckEventArgs[] args)
        {
            if (_removed == null)
                return;

            foreach (var arg in args)
                _removed(this, arg);
        }

        protected void OnChanged(params HealthCheckEventArgs[] args)
        {
            if (_changed == null)
                return;

            foreach (var arg in args)
                _changed(this, arg);
        }

        #endregion Implementation of IHealthCheckService

        #region Implementation of IDisposable

        /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
            _timer.Dispose();
        }

        #endregion Implementation of IDisposable

        #region Private Method

        private void Remove(IEnumerable<AddressModel> addressModels)
        {
            foreach (var addressModel in addressModels)
            {
                MonitorEntry value;
                var ipAddress = addressModel as IpAddressModel;
                _dictionary.TryRemove(new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port), out value);
            }
        }

        private void RemoveUnhealthyAddress(IEnumerable<MonitorEntry> monitorEntry)
        {
            if (monitorEntry.Any())
            {
                var addresses = monitorEntry.Select(p =>
                {
                    var ipEndPoint = p.EndPoint as IPEndPoint;
                    return new IpAddressModel(ipEndPoint.Address.ToString(), ipEndPoint.Port);
                }).ToList();
                _serviceRouteManager.RemveAddressAsync(addresses).Wait();
                addresses.ForEach(p =>
                {
                    var ipAddress = p as IpAddressModel;
                    _dictionary.TryRemove(new ValueTuple<string, int>(ipAddress.Ip, ipAddress.Port), out MonitorEntry value);
                });
                OnRemoved(addresses.Select(p => new HealthCheckEventArgs(p)).ToArray());
            }
        }

        private bool Check(AddressModel address, int timeout)
        {
            //bool isHealth = false;
            //using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { SendTimeout = timeout })
            //{
            //    try
            //    {
            //        await socket.ConnectAsync(address.CreateEndPoint());
            //        isHealth = true;
            //    }
            //    catch
            //    {
            //        _logger.LogDebug($"地址为{address.ToString()}的服务当前不可用");
            //    }
            //}
            //return isHealth;
            var ipEndPoint = address.CreateEndPoint() as IPEndPoint;
            return SocketCheck.TestConnection(ipEndPoint.Address, ipEndPoint.Port, timeout);
        }

        private void Check(IEnumerable<MonitorEntry> entrys, int timeout)
        {
            foreach (var entry in entrys)
            {
                //using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { SendTimeout = timeout })
                //{
                //    try
                //    {
                //        await socket.ConnectAsync(entry.EndPoint);
                //        entry.UnhealthyTimes = 0;
                //        entry.Health = true;
                //    }
                //    catch
                //    {
                //        entry.UnhealthyTimes++;
                //        entry.Health = false;
                //        _logger.LogDebug($"地址为{entry.EndPoint.ToString()}的服务当前不可用,不健康的次数为:{entry.UnhealthyTimes}");
                //    }
                //}
                var ipEndPoint = entry.EndPoint as IPEndPoint;
                if (SocketCheck.TestConnection(ipEndPoint.Address, ipEndPoint.Port, timeout))
                {
                    entry.UnhealthyTimes = 0;
                    entry.Health = true;
                }
                else
                {
                    entry.UnhealthyTimes++;
                    entry.Health = false;
                    _logger.LogDebug($"地址为{entry.EndPoint.ToString()}的服务当前不可用,不健康的次数为:{entry.UnhealthyTimes}");
                }
            }
        }

        #endregion Private Method

        #region Help Class

        protected class MonitorEntry
        {
            public MonitorEntry(AddressModel addressModel, bool health = true)
            {
                EndPoint = addressModel.CreateEndPoint();
                Health = health;

            }

            public int UnhealthyTimes { get; set; }

            public EndPoint EndPoint { get; set; }
            public bool Health { get; set; }
        }

        #endregion Help Class
    }
}