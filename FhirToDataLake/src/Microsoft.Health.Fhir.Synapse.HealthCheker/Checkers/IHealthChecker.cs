using Microsoft.Health.Fhir.Synapse.HealthCheker.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Checkers
{
    public interface IHealthChecker
    {
        string Name { get; }
        Task<HealthCheckResult> PerformHealthCheck(CancellationToken cancellationToken = default);
    }
}
