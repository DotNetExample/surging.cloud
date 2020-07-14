using Surging.Core.CPlatform.Address;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Surging.Core.Consul.Internal.Cluster.HealthChecks
{
    public interface IHealthCheckService
    {
        void Monitor(AddressModel address);
         
        Task<bool> IsHealth(AddressModel address);
    }
}
