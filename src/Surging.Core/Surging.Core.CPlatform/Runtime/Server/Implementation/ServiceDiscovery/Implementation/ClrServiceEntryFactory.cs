using Surging.Core.CPlatform.Convertibles;
using Surging.Core.CPlatform.DependencyResolution;
using Surging.Core.CPlatform.Filters.Implementation;
using Surging.Core.CPlatform.Ids;
using Surging.Core.CPlatform.Intercept;
using Surging.Core.CPlatform.Routing.Template;
using Surging.Core.CPlatform.Runtime.Server.Implementation.ServiceDiscovery.Attributes;
using Surging.Core.CPlatform.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using static Surging.Core.CPlatform.Utilities.FastInvoke;

namespace Surging.Core.CPlatform.Runtime.Server.Implementation.ServiceDiscovery.Implementation
{
    /// <summary>
    /// Clr������Ŀ������
    /// </summary>
    public class ClrServiceEntryFactory : IClrServiceEntryFactory
    {
        #region Field
        private readonly CPlatformContainer _serviceProvider;
        private readonly IServiceIdGenerator _serviceIdGenerator;
        private readonly ITypeConvertibleService _typeConvertibleService;
        #endregion Field

        #region Constructor
        public ClrServiceEntryFactory(CPlatformContainer serviceProvider, IServiceIdGenerator serviceIdGenerator, ITypeConvertibleService typeConvertibleService)
        {
            _serviceProvider = serviceProvider;
            _serviceIdGenerator = serviceIdGenerator;
            _typeConvertibleService = typeConvertibleService;
        }

        #endregion Constructor

        #region Implementation of IClrServiceEntryFactory

        /// <summary>
        /// ����������Ŀ��
        /// </summary>
        /// <param name="service">�������͡�</param>
        /// <param name="serviceImplementation">����ʵ�����͡�</param>
        /// <returns>������Ŀ���ϡ�</returns>
        public IEnumerable<ServiceEntry> CreateServiceEntry(Type service)
        {
            var routeTemplate = service.GetCustomAttribute<ServiceBundleAttribute>();
            foreach (var methodInfo in service.GetTypeInfo().GetMethods())
            {
                var serviceRoute = methodInfo.GetCustomAttribute<ServiceRouteAttribute>();
                var routeTemplateVal = routeTemplate.RouteTemplate;
                if (!routeTemplate.IsPrefix && serviceRoute != null)
                    routeTemplateVal = serviceRoute.Template;
                else if (routeTemplate.IsPrefix && serviceRoute != null)
                {

                    var prefixRouteTemplate = routeTemplate.RouteTemplate;
                    if (prefixRouteTemplate.Contains("{method}", StringComparison.OrdinalIgnoreCase))
                    {
                        prefixRouteTemplate = prefixRouteTemplate.Replace("{method}", "", StringComparison.OrdinalIgnoreCase).TrimEnd('/');
                    }
                    routeTemplateVal = $"{ prefixRouteTemplate}/{ serviceRoute.Template}";
                }
                yield return Create(methodInfo, service.Name, routeTemplateVal, serviceRoute != null);
            }
        }
        #endregion Implementation of IClrServiceEntryFactory

        #region Private Method

        private ServiceEntry Create(MethodInfo method, string serviceName, string routeTemplate, bool routeIsReWriteByServiceRoute = false)
        {
            var serviceId = _serviceIdGenerator.GenerateServiceId(method);
            var attributes = method.GetCustomAttributes().ToList();
            var serviceDescriptor = new ServiceDescriptor
            {
                Id = serviceId,
                RoutePath = RoutePatternParser.Parse(routeTemplate, serviceName, method.Name, routeIsReWriteByServiceRoute)
            };
            var httpMethodAttributes = attributes.Where(p => p is HttpMethodAttribute).Select(p => p as HttpMethodAttribute).ToList();
            var httpMethods = new List<string>();
            StringBuilder httpMethod = new StringBuilder();
            foreach (var attribute in httpMethodAttributes)
            {
                httpMethods.AddRange(attribute.HttpMethods);
                if (attribute.IsRegisterMetadata)
                    httpMethod.AppendJoin(',', attribute.HttpMethods).Append(",");
            }
            if (httpMethod.Length > 0)
            {
                httpMethod.Length = httpMethod.Length - 1;
                serviceDescriptor.HttpMethod(httpMethod.ToString());
            }
            var authorization = attributes.Where(p => p is AuthorizationFilterAttribute).FirstOrDefault();
            if (authorization != null)
                serviceDescriptor.EnableAuthorization(true);
            if (authorization != null)
            {
                serviceDescriptor.AuthType(((authorization as AuthorizationAttribute)?.AuthType)
                    ?? AuthorizationType.AppSecret);
            }
            else
            {
                serviceDescriptor.EnableAuthorization(true);
                serviceDescriptor.AuthType(AuthorizationType.JWT);
            }

            var descriptorAttributes = method.GetCustomAttributes<ServiceDescriptorAttribute>();
            foreach (var descriptorAttribute in descriptorAttributes)
            {
                descriptorAttribute.Apply(serviceDescriptor);
            }
            var fastInvoker = GetHandler(serviceId, method);
            return new ServiceEntry
            {
                Descriptor = serviceDescriptor,
                RoutePath = serviceDescriptor.RoutePath,
                Methods = httpMethods,
                MethodName = method.Name,
                Type = method.DeclaringType,
                Attributes = attributes,
                ParamTypes = GetParamTypes(method),
                CacheKeys = GetCackeKeys(method),
                Func = (key, parameters) =>
                {
                    object instance = null;
                    if (AppConfig.ServerOptions.IsModulePerLifetimeScope)
                        instance = _serviceProvider.GetInstancePerLifetimeScope(key, method.DeclaringType);
                    else
                        instance = _serviceProvider.GetInstances(key, method.DeclaringType);
                    var list = new List<object>();

                    foreach (var parameterInfo in method.GetParameters())
                    {
                        if (parameters.ContainsKey(parameterInfo.Name))
                        {
                            var value = parameters[parameterInfo.Name];
                            var parameterType = parameterInfo.ParameterType;
                            var parameter = _typeConvertibleService.Convert(value, parameterType);
                            list.Add(parameter);
                        }
                        //�����Ƿ���Ĭ��ֵ���жϣ���Ĭ��ֵ�������û�û����ȡĬ��ֵ
                        else if (parameterInfo.HasDefaultValue && !parameters.ContainsKey(parameterInfo.Name))
                        {
                            list.Add(parameterInfo.DefaultValue);
                        }
                        else
                        {
                            list.Add(null);
                        }
                    }
                    var result = fastInvoker(instance, list.ToArray());
                    return Task.FromResult(result);
                }
            };
        }

        private IEnumerable<string> GetCackeKeys(MethodInfo method)
        {

            var result = new Dictionary<int, string>();
            var parameters = method.GetParameters();
            var flag = 0;
            foreach (var parameter in parameters)
            {
                if (typeof(IEnumerable<>).IsAssignableFrom(parameter.ParameterType))
                {
                    continue;
                }
                if (parameter.ParameterType.IsClass && parameter.ParameterType != typeof(string) && flag == 0)
                {
                    var runtimeProperties = parameter.ParameterType.GetRuntimeProperties();
                    foreach (var runtimePropertie in runtimeProperties)
                    {
                        var cacheKeyAttr = runtimePropertie.GetCustomAttribute<KeyAttribute>();
                        if (cacheKeyAttr != null)
                        {
                            result.Add(cacheKeyAttr.SortIndex, runtimePropertie.Name);
                        }
                    }
                }
                else
                {
                    var cacheKeyAttr = parameter.GetCustomAttribute<KeyAttribute>();
                    if (cacheKeyAttr != null)
                    {
                        result.Add(cacheKeyAttr.SortIndex, parameter.Name);
                    }
                }
                flag++;
            }
            return result.OrderBy(p => p.Key).Select(p => p.Value);
        }

        private IDictionary<string, Type> GetParamTypes(MethodInfo method)
        {
            var parameterDic = new Dictionary<string, Type>();
            var parameters = method.GetParameters();
            foreach (var parameter in parameters)
            {
                parameterDic.Add(parameter.Name, parameter.ParameterType);
            }
            return parameterDic;
        }

        private FastInvokeHandler GetHandler(string key, MethodInfo method)
        {
            var objInstance = ServiceResolver.Current.GetService(null, key);
            if (objInstance == null)
            {
                objInstance = FastInvoke.GetMethodInvoker(method);
                ServiceResolver.Current.Register(key, objInstance, null);
            }
            return objInstance as FastInvokeHandler;
        }
        #endregion Private Method
    }
}
