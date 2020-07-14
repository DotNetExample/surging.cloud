using Microsoft.Extensions.Logging;
using Surging.Core.CPlatform.Exceptions;
using Surging.Core.CPlatform.Runtime.Server;
using Surging.Core.CPlatform.Transport.Implementation;
using Surging.Core.CPlatform.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Surging.Core.CPlatform.Routing.Implementation
{
    public class DefaultServiceRouteProvider : IServiceRouteProvider
    {
        private readonly ConcurrentDictionary<string, ServiceRoute> _concurrent = new ConcurrentDictionary<string, ServiceRoute>();

        private readonly List<ServiceRoute> _localRoutes = new List<ServiceRoute>();

        private readonly ConcurrentDictionary<string, ServiceRoute> _serviceRoute = new ConcurrentDictionary<string, ServiceRoute>();

        private readonly IServiceEntryManager _serviceEntryManager;
        private readonly ILogger<DefaultServiceRouteProvider> _logger;
        private readonly IServiceRouteManager _serviceRouteManager;
        private readonly IServiceTokenGenerator _serviceTokenGenerator;
        public DefaultServiceRouteProvider(IServiceRouteManager serviceRouteManager, ILogger<DefaultServiceRouteProvider> logger,
            IServiceEntryManager serviceEntryManager, IServiceTokenGenerator serviceTokenGenerator)
        {
            _serviceRouteManager = serviceRouteManager;
            serviceRouteManager.Changed += ServiceRouteManager_Removed;
            serviceRouteManager.Removed += ServiceRouteManager_Removed;
            serviceRouteManager.Created += ServiceRouteManager_Add;
            _serviceEntryManager = serviceEntryManager;
            _serviceTokenGenerator = serviceTokenGenerator;
            _logger = logger;
        }

        public async Task<ServiceRoute> Locate(string serviceId)
        {
            _concurrent.TryGetValue(serviceId, out ServiceRoute route);
            if (route == null)
            {
                var routes = await _serviceRouteManager.GetRoutesAsync();
                route = routes.FirstOrDefault(i => i.ServiceDescriptor.Id == serviceId);
                if (route == null)
                {
                    if (_logger.IsEnabled(LogLevel.Warning))
                        _logger.LogWarning($"根据服务id：{serviceId}，找不到相关服务信息。");
                }
                else
                    _concurrent.GetOrAdd(serviceId, route);
            }
            return route;
        }

        public async Task<ServiceRoute> GetLocalRouteByPath(string path)
        {
            var addess = NetUtils.GetHostAddress();

            if (_localRoutes.Any())
            {
                _localRoutes.AddRange(_serviceEntryManager.GetEntries().Select(i =>
                {
                    i.Descriptor.Token = _serviceTokenGenerator.GetToken();
                    return new ServiceRoute
                    {
                        Address = new[] { addess },
                        ServiceDescriptor = i.Descriptor
                    };
                }).ToList());
            }

            path = path.ToLower();
            _serviceRoute.TryGetValue(path, out ServiceRoute route);
            if (route == null)
            {
                return await GetRouteByRegexPathAsync(_localRoutes, path);
            }
            else
            {
                return route;
            }

        }

        public async Task<ServiceRoute> GetLocalRouteByRegexPath(string path)
        {
            var addess = NetUtils.GetHostAddress();

            if (_localRoutes.Count == 0)
            {
                _localRoutes.AddRange(_serviceEntryManager.GetEntries().Select(i =>
                {
                    i.Descriptor.Token = _serviceTokenGenerator.GetToken();
                    return new ServiceRoute
                    {
                        Address = new[] { addess },
                        ServiceDescriptor = i.Descriptor
                    };
                }).ToList());
            }

            path = path.ToLower();
            _serviceRoute.TryGetValue(path, out ServiceRoute route);
            if (route == null)
            {
                return await GetRouteByRegexPathAsync(_localRoutes, path);
            }
            else
            {
                return route;
            }
        }

        public async Task<ServiceRoute>  GetRouteByPath(string path)
        {
            _serviceRoute.TryGetValue(path.ToLower(), out ServiceRoute route);
            if (route == null)
            {
                return await GetRouteByPathAsync(path);
            }
            return route;
        }

        public async Task<ServiceRoute> GetRouteByRegexPath(string path)
        {
            path = path.ToLower();
            _serviceRoute.TryGetValue(path, out ServiceRoute route);
            if (route == null)
            {
                route = await _serviceRouteManager.GetRouteByPathAsync(path);
                if (route == null)
                {
                    if (_logger.IsEnabled(LogLevel.Warning))
                    {
                        _logger.LogWarning($"根据服务路由路径：{path}，找不到相关服务信息。");
                        if (route == null)
                        {
                            throw new CPlatformException($"根据服务路由路径：{path}，找不到相关服务信息。");
                        }
                    }

                }
                return route;
            }
            else
            {
                return route;
            }
        }

        public async Task<ServiceRoute> GetRouteByPathOrRegexPath(string path) 
        {
            var route = await GetRouteByPath(path);
          
            return route;
        }


        public async Task<ServiceRoute> SearchRoute(string path)
        {
            return await SearchRouteAsync(path);
        }

        public async Task RegisterRoutes(decimal processorTime)
        {
            var addess = NetUtils.GetHostAddress();
            addess.ProcessorTime = processorTime;
            RpcContext.GetContext().SetAttachment("Host", addess);
            var addressDescriptors = _serviceEntryManager.GetEntries().Select(i =>
            {
                i.Descriptor.Token = _serviceTokenGenerator.GetToken();
                return new ServiceRoute
                {
                    Address = new[] { addess },
                    ServiceDescriptor = i.Descriptor
                };
            }).ToList();
            await _serviceRouteManager.SetRoutesAsync(addressDescriptors);
        }

        #region 私有方法
        private static string GetCacheKey(ServiceDescriptor descriptor)
        {
            return descriptor.Id;
        }

        private void ServiceRouteManager_Removed(object sender, ServiceRouteEventArgs e)
        {
            var key = GetCacheKey(e.Route.ServiceDescriptor);
            ServiceRoute value;
            _concurrent.TryRemove(key, out value);
            _serviceRoute.TryRemove(e.Route.ServiceDescriptor.RoutePath, out value);
        }

        private void ServiceRouteManager_Add(object sender, ServiceRouteEventArgs e)
        {
            var key = GetCacheKey(e.Route.ServiceDescriptor);
            _concurrent.GetOrAdd(key, e.Route);
            _serviceRoute.GetOrAdd(e.Route.ServiceDescriptor.RoutePath, e.Route);
        }

        private async Task<ServiceRoute> SearchRouteAsync(string path)
        {
            var route = await _serviceRouteManager.GetRouteByPathAsync(path);
            if (route == null)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning($"根据服务路由路径：{path}，找不到相关服务信息。");
                return null;
            }
            else
            {
                _serviceRoute.GetOrAdd(path, route);
            }
            return route;
        }

        private async Task<ServiceRoute> GetRouteByPathAsync(string path)
        {
            var route = await _serviceRouteManager.GetRouteByPathAsync(path);
            if (route == null)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning($"根据服务路由路径：{path}，找不到相关服务信息。");
                return null;
            }
            
            else
            {
                _serviceRoute.GetOrAdd(path, route);
            }      
            return route;
        }

        private async Task<ServiceRoute> GetRouteByRegexPathAsync(IEnumerable<ServiceRoute> routes, string path)
        {
            var pattern = "/{.*?}";

            var route = routes.FirstOrDefault(i =>
            {
                var routePath = Regex.Replace(i.ServiceDescriptor.RoutePath, pattern, "");
                var newPath = path.Replace(routePath, "");
                return ((newPath.StartsWith("/") || newPath.Length == 0) && i.ServiceDescriptor.RoutePath.Split("/").Length == path.Split("/").Length && !i.ServiceDescriptor.GetMetadata<bool>("IsOverload"))
                ;
            });


            if (route == null)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning($"根据服务路由路径：{path}，找不到相关服务信息。");
            }
            else 
            {
                if (Regex.IsMatch(route.ServiceDescriptor.RoutePath, pattern))
                {
                    _serviceRoute.GetOrAdd(path, route);
                }
            }
            
            return route;
        }

        #endregion
    }
}