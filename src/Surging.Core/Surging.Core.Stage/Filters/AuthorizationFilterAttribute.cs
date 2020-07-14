using Surging.Core.ApiGateWay.OAuth;
using Surging.Core.CPlatform;
using Surging.Core.CPlatform.Filters.Implementation;
using Surging.Core.CPlatform.Messages;
using Surging.Core.CPlatform.Transport.Implementation;
using Surging.Core.CPlatform.Utilities;
using Surging.Core.KestrelHttpServer.Filters;
using Surging.Core.KestrelHttpServer.Filters.Implementation;
using System.Threading.Tasks;
using Autofac;
using System;
using Surging.Core.ProxyGenerator;
using System.Collections.Generic;
using Surging.Core.CPlatform.Routing;
using System.Linq;
using System.Security.Claims;

namespace Surging.Core.Stage.Filters
{
    public class AuthorizationFilterAttribute : IAuthorizationFilter
    {
        private readonly IAuthorizationServerProvider _authorizationServerProvider;
        private readonly IServiceProxyProvider _serviceProxyProvider;
        private readonly IServiceRouteProvider _serviceRouteProvider;
        private const int _order = int.MaxValue;
        public AuthorizationFilterAttribute()
        {
            _authorizationServerProvider = ServiceLocator.Current.Resolve<IAuthorizationServerProvider>();
            _serviceProxyProvider = ServiceLocator.Current.Resolve<IServiceProxyProvider>();
            _serviceRouteProvider = ServiceLocator.Current.Resolve<IServiceRouteProvider>();
        }

        public int Order { get { return _order; } }

        public async Task OnAuthorization(AuthorizationFilterContext filterContext)
        {
            var gatewayAppConfig = AppConfig.Options.ApiGetWay;
           
            if (filterContext.Route != null && filterContext.Route.ServiceDescriptor.DisableNetwork())
            {
                var actionName = filterContext.Route.ServiceDescriptor.GroupName().IsNullOrEmpty() ? filterContext.Route.ServiceDescriptor.RoutePath : filterContext.Route.ServiceDescriptor.GroupName();
                filterContext.Result = new HttpResultMessage<object> { IsSucceed = false, StatusCode = CPlatform.Exceptions.StatusCode.UnAuthorized, Message = $"{actionName}禁止被外网访问" };
            }
            else
            {
                var token = filterContext.Context.Request.Headers["Authorization"];

                if (filterContext.Route != null)
                {
                    if (filterContext.Route.ServiceDescriptor.AuthType() == AuthorizationType.JWT.ToString())
                    {

                        if (token.Any() && token.Count >= 1)
                        {
                            var isSuccess = await _authorizationServerProvider.ValidateClientAuthentication(token);
                            if (!isSuccess && filterContext.Route.ServiceDescriptor.EnableAuthorization())
                            {
                                filterContext.Result = new HttpResultMessage<object> { IsSucceed = false, StatusCode = CPlatform.Exceptions.StatusCode.UnAuthentication, Message = "Token凭证不合法或登录超时" };
                            }

                            dynamic payload = _authorizationServerProvider.GetPayload(token);
                            //RpcContext.GetContext().SetAttachment(Surging.Core.CPlatform.AppConfig.PayloadKey, payload);

                            var userId = payload.userId ?? payload.UserId;
                            var userName = payload.userName ?? payload.UserName;
                            var claimsIdentity = new ClaimsIdentity();
                            if (userId != null) 
                            {
                                claimsIdentity.AddClaim(new Claim("userId",userId.ToString()));
                            }
                            if (userName != null) 
                            {
                                claimsIdentity.AddClaim(new Claim("userName", userName.ToString()));
                            }
                            filterContext.Context.User = new ClaimsPrincipal(claimsIdentity);

                             if (!gatewayAppConfig.AuthorizationRoutePath.IsNullOrEmpty() && filterContext.Route.ServiceDescriptor.EnableAuthorization())
                            {
                                var rpcParams = new Dictionary<string, object>() {
                                        {  "serviceId", filterContext.Route.ServiceDescriptor.Id }
                                    };
                                var authorizationRoutePath = await _serviceRouteProvider.GetRouteByPathOrRegexPath(gatewayAppConfig.AuthorizationRoutePath);
                                if (authorizationRoutePath == null)
                                {
                                    filterContext.Result = new HttpResultMessage<object> { IsSucceed = false, StatusCode = CPlatform.Exceptions.StatusCode.RequestError, Message = "没有找到实现接口鉴权的WebApi的路由信息" };
                                    return;
                                }
                                var isPermission = await _serviceProxyProvider.Invoke<bool>(rpcParams, gatewayAppConfig.AuthorizationRoutePath, gatewayAppConfig.AuthorizationServiceKey);
                                if (!isPermission)
                                {
                                    var actionName = filterContext.Route.ServiceDescriptor.GroupName().IsNullOrEmpty() ? filterContext.Route.ServiceDescriptor.RoutePath : filterContext.Route.ServiceDescriptor.GroupName();
                                    filterContext.Result = new HttpResultMessage<object> { IsSucceed = false, StatusCode = CPlatform.Exceptions.StatusCode.RequestError, Message = $"没有请求{actionName}的权限" };
                                }
                            }
                        }
                        else
                        {
                            if (filterContext.Route.ServiceDescriptor.EnableAuthorization())
                            {
                                filterContext.Result = new HttpResultMessage<object> { IsSucceed = false, StatusCode = CPlatform.Exceptions.StatusCode.UnAuthentication, Message = $"请先登录系统" };
                            }
                        }

                    }
                    else 
                    {
                        if (filterContext.Route.ServiceDescriptor.EnableAuthorization())
                        {
                            filterContext.Result = new HttpResultMessage<object> { IsSucceed = false, StatusCode = CPlatform.Exceptions.StatusCode.UnAuthentication, Message = $"暂不支持{filterContext.Route.ServiceDescriptor.AuthType()}类型的身份认证方式" };
                        }
                    }
                    
                }
            }

            if (String.Compare(filterContext.Path.ToLower(), gatewayAppConfig.TokenEndpointPath, true) == 0)
            {
                filterContext.Context.Items.Add("path", gatewayAppConfig.AuthenticationRoutePath);
            }           
        }
    }
}
 
