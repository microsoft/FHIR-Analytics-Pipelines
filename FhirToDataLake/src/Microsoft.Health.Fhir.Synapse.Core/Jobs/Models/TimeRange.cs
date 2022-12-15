using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class TimeRange
    {

        /// <summary>
        /// Start time, process all data if not specified,
        /// </summary>
        public DateTimeOffset? DataStartTime { get; set; }

        /// <summary>
        /// End time
        /// </summary>
        public DateTimeOffset DataEndTime { get; set; }
    }
}