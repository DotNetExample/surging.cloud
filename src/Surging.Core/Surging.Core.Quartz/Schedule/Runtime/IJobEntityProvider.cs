using System.Collections.Generic;

namespace Surging.Core.Quartz.Schedule.Runtime
{
    public interface IJobEntityProvider
    {
        IEnumerable<JobEntity> GetJobEntities();
    }
}
