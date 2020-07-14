﻿using Autofac;
using Microsoft.Extensions.Logging;
using Surging.Core.Caching;
using Surging.Core.Caching.Configurations;
using Surging.Core.Codec.MessagePack;
using Surging.Core.Consul;
using Surging.Core.Consul.Configurations;
using Surging.Core.CPlatform;
using Surging.Core.CPlatform.Configurations;
using Surging.Core.CPlatform.Utilities;
using Surging.Core.DotNetty;
using Surging.Core.EventBusKafka.Configurations;
//using Surging.Core.EventBusKafka;
using Surging.Core.Log4net;
using Surging.Core.Nlog;
using Surging.Core.Protocol.Http;
using Surging.Core.ProxyGenerator;
using Surging.Core.ServiceHosting;
using Surging.Core.ServiceHosting.Internal.Implementation;
using Surging.Core.System.Intercept;
using Surging.Core.Zookeeper.Configurations;
using System;
//using Surging.Core.Zookeeper;
//using Surging.Core.Zookeeper.Configurations;
using System.Text;

namespace Surging.Services.Server
{
    public class Program
    {
        static void Main(string[] args)
        {
            //  Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            var host = new ServiceHostBuilder()
                .RegisterServices(builder =>
                {
                    builder.AddMicroService(option =>
                    {
                        option.AddServiceRuntime()
                        .AddRelateService()
                        .AddConfigurationWatch()
                        //option.UseZooKeeperManager(new ConfigInfo("127.0.0.1:2181")); 
                        .AddServiceEngine(typeof(SurgingServiceEngine))
                        .AddClientIntercepted(typeof(CacheProviderInterceptor));
                        builder.Register(p => new CPlatformContainer(ServiceLocator.Current));
                    });
                })
                .ConfigureLogging(logger =>
                {
                    logger.AddConfiguration(
                        Core.CPlatform.AppConfig.GetSection("Logging"));
                })
                .UseServer(options => { })
                .UseConsoleLifetime()
                .Configure(build =>
                {

#if DEBUG
                    build.AddCacheFile("${cachePath}|/app/configs/cacheSettings.json", optional: false, reloadOnChange: true);
                    build.AddCPlatformFile("${surgingPath}|/app/configs/surgingSettings.json", optional: false, reloadOnChange: true);
                    build.AddEventBusFile("${eventBusPath}|/app/configs/eventBusSettings.json", optional: false, reloadOnChange: true);
                    build.AddConsulFile("${consulPath}|/app/configs/consul.json", optional: false, reloadOnChange: true);
                    build.AddZookeeperFile("${zookeeperPath}|/app/configs/zookeeper.json", optional: false, reloadOnChange: true);

#else
                    build.AddCacheFile("${cachePath}|configs/cacheSettings.json", optional: false, reloadOnChange: true);
                    build.AddCPlatformFile("${surgingPath}|configs/surgingSettings.json", optional: false, reloadOnChange: true);
                    build.AddEventBusFile("${eventBusPath}|configs/eventBusSettings.json", optional: false);
                    build.AddConsulFile("${consulPath}|configs/consul.json", optional: false, reloadOnChange: true);
                    build.AddZookeeperFile("${zookeeperPath}|configs/zookeeper.json", optional: false, reloadOnChange: true);
#endif
                })
                .UseStartup<Startup>()
                .Build();

            using (host.Run())
            {
                Console.WriteLine($"服务端启动成功，{DateTime.Now}。");
            }
        }
    }
}
