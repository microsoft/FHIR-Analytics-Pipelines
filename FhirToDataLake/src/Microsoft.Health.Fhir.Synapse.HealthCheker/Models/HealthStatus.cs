using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Models
{
    public class HealthStatus
    {
        public DateTime StartTime { get; set; } = DateTime.UtcNow;

        public DateTime EndTime { get; set; } = DateTime.UtcNow;

        public IList<HealthCheckResult> HealthCheckResults { get; set; }
    }
}
