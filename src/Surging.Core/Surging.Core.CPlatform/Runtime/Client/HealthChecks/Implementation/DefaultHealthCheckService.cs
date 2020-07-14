using Microsoft.Extensions.Logging;
using Surging.Core.CPlatform.Address;
using Surging.Core.CPlatform.Exceptions;
using Surging.Core.CPlatform.Routing;
using Surging.Core.CPlatform.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Timers;

namespace Surging.Core.CPlatform.Runtime.Client.HealthChecks.Implementation
{
    /// <summary>
    /// 默认健康检查服务(每10秒会检查一次服务状态，在构造函数中添加服务管理事件) 
    /// </summary>
    public class DefaultHealthCheckService : IHealthCheckService, IDisposable
    {
        private readonly ConcurrentDictionary<Tuple<string, int>, MonitorEntry> _dictionary =
            new ConcurrentDictionary<Tuple<string, int>, MonitorEntry>();
        private readonly IServiceRouteManager _serviceRouteManager;
        private readonly int _timeout = AppConfig.ServerOptions.HealthCheckTimeout;
        private readonly Timer _timer;
        private EventHandler<HealthCheckEventArgs> _removed;

        private EventHandler<HealthCheckEventArgs> _changed;
        private static ILogger<DefaultHealthCheckService> _logger;
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
            var timeSpan = TimeSpan.FromSeconds(AppConfig.ServerOptions.HealthCheckWatchIntervalInSeconds);

            _serviceRouteManager = serviceRouteManager;
            //建立计时器
            _timer = new Timer(timeSpan.TotalMilliseconds);
            _timer.Enabled = true;
            _timer.AutoReset = true;
            _timer.Elapsed += async (sender, e)=> 
            {
                //检查服务是否可用
                await Check(_dictionary.ToArray().Select(i => i.Value), _timeout);
                //移除不可用的服务地址
                RemoveUnhealthyAddress(_dictionary.ToArray().Select(i => i.Value).Where(m => m.UnhealthyCount >= AppConfig.ServerOptions.AllowServerUnhealthyTimes && m.TimeOutCount >= AppConfig.ServerOptions.AllowServerUnhealthyTimes));
            
            };
            _timer.Start();
            //去除监控。
            serviceRouteManager.Removed += (s, e) =>
            {
                Remove(e.Route.Address);
            };
            //重新监控。
            serviceRouteManager.Created += async (s, e) =>
            {
                var hostAddr = NetUtils.GetHostAddress() as IpAddressModel;
                foreach (var address in e.Route.Address) 
                {
                    var ipAddress = address as IpAddressModel;
                    if (ipAddress != hostAddr) 
                    {
                        await Monitor(ipAddress);
                    }
                }
                
            };
            //重新监控。
            serviceRouteManager.Changed += async (s, e) =>
            {
                var hostAddr = NetUtils.GetHostAddress() as IpAddressModel;
                foreach (var address in e.Route.Address)
                {
                    var ipAddress = address as IpAddressModel;
                    if (ipAddress != hostAddr) 
                    {
                        await Monitor(ipAddress);
                    }
                }
            };
        }


        #region Implementation of IHealthCheckService

        /// <summary>
        /// 监控一个地址。
        /// </summary>
        /// <param name="address">地址模型。</param>
        /// <returns>一个任务。</returns>
        public async Task Monitor(AddressModel address)
        {
            var ipAddress = address as IpAddressModel;
            if (!_dictionary.TryGetValue(new Tuple<string, int>(ipAddress.Ip, ipAddress.Port), out MonitorEntry monitorEntry)) 
            {
                monitorEntry = new MonitorEntry(ipAddress);
                await Check(monitorEntry,_timeout);
                _dictionary.TryAdd(new Tuple<string, int>(ipAddress.Ip, ipAddress.Port), monitorEntry);
            }
            OnChanged(new HealthCheckEventArgs(address, monitorEntry.Health));
        }

        /// <summary>
        /// 判断一个地址是否健康。
        /// </summary>
        /// <param name="address">地址模型。</param>
        /// <returns>健康返回true，否则返回false。</returns>
        public async Task<bool> IsHealth(AddressModel address)
        {
            var ipAddress = address as IpAddressModel;
            MonitorEntry entry;
            if (!_dictionary.TryGetValue(new Tuple<string, int>(ipAddress.Ip, ipAddress.Port), out entry))
            {
                entry = new MonitorEntry(address);
                await Check(entry, _timeout);
                _dictionary.TryAdd(new Tuple<string, int>(ipAddress.Ip, ipAddress.Port), entry);
            }
            if (entry.UnhealthyCount >= AppConfig.ServerOptions.AllowServerUnhealthyTimes || entry.TimeOutCount >= AppConfig.ServerOptions.AllowServerUnhealthyTimes) 
            {
                RemoveUnhealthyAddress(entry);
            }
            OnChanged(new HealthCheckEventArgs(address, entry.Health));
            return entry.Health;

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
                var entry = _dictionary.GetOrAdd(new Tuple<string, int>(ipAddress.Ip, ipAddress.Port), k => new MonitorEntry(address));
                entry.Health = false;
                entry.UnhealthyCount += 1;
            });
        }

        public Task MarkFailureForTimeOut(AddressModel address)
        {
            return Task.Run(() =>
            {
                var ipAddress = address as IpAddressModel;
                var entry = _dictionary.GetOrAdd(new Tuple<string, int>(ipAddress.Ip, ipAddress.Port), k => new MonitorEntry(address));
                entry.Health = false;
                entry.UnhealthyCount += 1;
                entry.TimeOutCount += 1;
                entry.TimeOutTime = DateTime.Now;
            });
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
                _dictionary.TryRemove(new Tuple<string, int>(ipAddress.Ip, ipAddress.Port), out value);
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
                if (addresses.Any()) 
                {
                    _serviceRouteManager.RemveAddressAsync(addresses).Wait();
                    addresses.ForEach(p => {
                        var ipAddress = p as IpAddressModel;
                        _dictionary.TryRemove(new Tuple<string, int>(ipAddress.Ip, ipAddress.Port), out MonitorEntry value);
                    });
                    OnRemoved(addresses.Select(p => new HealthCheckEventArgs(p)).ToArray());
                }
                
            }
        }


        private void RemoveUnhealthyAddress(MonitorEntry monitorEntry)
        {
            var ipEndPoint = monitorEntry.EndPoint as IPEndPoint;
            var address = new IpAddressModel(ipEndPoint.Address.ToString(), ipEndPoint.Port);
            _serviceRouteManager.RemveAddressAsync(new List<AddressModel>() { address }).Wait();
            _dictionary.TryRemove(new Tuple<string, int>(address.Ip, address.Port), out MonitorEntry value);
            OnRemoved(new HealthCheckEventArgs(address));
        }

        private static async Task<bool> CheckIpHealth(IPAddress ip, int timeout)
        {
            bool isHealth = false;
            try
            {
                var ping = new Ping();
                var pingReply = ping.Send(ip, timeout);
                isHealth = pingReply.Status == IPStatus.Success;
                return isHealth;
            }
            catch
            {
                _logger.LogDebug($"地址为{ip}的主机当前无法连接");
            }
            return isHealth;

        }
        private static async Task Check(MonitorEntry entry, int timeout)
        {
            var ipEndPoint = entry.EndPoint as IPEndPoint;
            var isHealth = await CheckIpHealth(ipEndPoint.Address, timeout);
            if (isHealth)
            {
                TcpClient client = new TcpClient();
                try
                {
                    var endpont = (IPEndPoint)entry.EndPoint;
                    var ar = client.BeginConnect(endpont.Address, endpont.Port, null, null);
                    ar.AsyncWaitHandle.WaitOne(timeout);
                    if (client.Connected)
                    {
                        entry.UnhealthyCount = 0;
                        entry.Health = true;
                        if (entry.TimeOutCount > 0 && entry.TimeOutTime.HasValue && (DateTime.Now - entry.TimeOutTime.Value).TotalDays > 1)
                        {
                            entry.TimeOutCount = 0;
                            entry.TimeOutTime = null;
                        }
                    }
                    else
                    {
                        _logger.LogWarning($"服务{entry.EndPoint.ToString()}不健康");
                        entry.UnhealthyCount++;
                        entry.Health = false;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"服务{entry.EndPoint.ToString()}不健康,原因:{ex.Message}");
                    entry.UnhealthyCount++;
                    entry.Health = false;
                }
                finally
                {
                    client.Close();
                }
            }
            else 
            {
                entry.UnhealthyCount++;
                entry.Health = false;
            }
            
        }

        private static async Task Check(IEnumerable<MonitorEntry> entrys, int timeout)
        {
            foreach (var entry in entrys)
            {
                var ipEndPoint = entry.EndPoint as IPEndPoint;
                var isHealth = await CheckIpHealth(ipEndPoint.Address, timeout);
                if (isHealth)
                {
                    TcpClient client = new TcpClient();
                    try
                    {
                        var endpont = (IPEndPoint)entry.EndPoint;
                        var ar = client.BeginConnect(endpont.Address, endpont.Port, null, null);
                        ar.AsyncWaitHandle.WaitOne(timeout);
                        if (client.Connected)
                        {
                            entry.UnhealthyCount = 0;
                            entry.Health = true;
                            if (entry.TimeOutCount > 0 && entry.TimeOutTime.HasValue && (DateTime.Now - entry.TimeOutTime.Value).TotalDays > 1) 
                            {
                                entry.TimeOutCount = 0;
                                entry.TimeOutTime = null;
                            }
                        }
                        else
                        {
                            _logger.LogWarning($"服务{entry.EndPoint.ToString()}不健康");
                            entry.UnhealthyCount++;
                            entry.Health = false;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"服务{entry.EndPoint.ToString()}不健康,原因:{ex.Message}");
                        entry.UnhealthyCount++;
                        entry.Health = false;
                    }
                    finally
                    {
                        client.Close();
                    }
                }
                else
                {
                    entry.UnhealthyCount++;
                    entry.Health = false;
                }

            }
        }

        #endregion Private Method

        #region Help Class

        protected class MonitorEntry
        {
            public MonitorEntry(AddressModel addressModel)
            {
                EndPoint = addressModel.CreateEndPoint();
                Health = false;
                UnhealthyCount = 0;
                TimeOutCount = 0;

            }

            public int UnhealthyCount { get; set; }

            public int TimeOutCount { get; set; }

            public DateTime? TimeOutTime { get; set; }

            public EndPoint EndPoint { get; set; }

            public bool Health { get; set; }
        }

        #endregion Help Class
    }
}