using Quartz;
using Surging.Core.CPlatform.Ioc;

namespace Surging.Core.Quartz.Schedule
{
    public interface ICronTriggerFactory : ITransientDependency
    {
        ITrigger CreateTrigger(string cronExpression);
    }
}
