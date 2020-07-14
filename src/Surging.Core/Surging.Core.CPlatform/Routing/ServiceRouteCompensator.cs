using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Timers;

namespace Surging.Core.CPlatform.Routing
{
    public class ServiceRouteCompensator : IDisposable
    {
        private readonly Timer _timer;
        private ILogger<ServiceRouteCompensator> _logger;
        private IServiceRouteProvider _serviceRouteProvider;

        public ServiceRouteCompensator(ILogger<ServiceRouteCompensator> logger, IServiceRouteProvider serviceRouteProvider)
        {
            _logger = logger;
            _serviceRouteProvider = serviceRouteProvider;
            _timer = new Timer();
            _timer.Enabled = true;

            _timer.Interval = AppConfig.ServerOptions.ServiceRouteWatchIntervalInMinutes * 1000 * 60;
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();
            
            
        }

        public void Dispose()
        {
            if (_timer != null) 
            {
                _timer.Stop();
                _timer.Dispose();
            }
            
        }

        private int GenerateInterval()
        {
            var ro = new Random();
            var interval = ro.Next(10000, 50000);
            return interval;
        }

        private void _timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            _serviceRouteProvider.RegisterRoutes(Math.Round(Convert.ToDecimal(Process.GetCurrentProcess().TotalProcessorTime.TotalSeconds), 2, MidpointRounding.AwayFromZero)).Wait();
            

        }
    }
}
