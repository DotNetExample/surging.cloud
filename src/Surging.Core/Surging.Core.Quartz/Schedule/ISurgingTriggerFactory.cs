using Quartz;
using Surging.Core.Quartz.Configurations;

namespace Surging.Core.Quartz.Schedule
{
    public interface ISurgingTriggerFactory
    {
        ITrigger CreateTrigger(JobOption jobOption);
    }
}
