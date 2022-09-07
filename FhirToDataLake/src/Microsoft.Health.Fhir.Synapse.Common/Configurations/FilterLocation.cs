using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class FilterLocation
    {
        /// <summary>
        /// The filter scope, "System" and "Group" are supported now.
        /// </summary>
        [JsonProperty("enableExternalFilter")]
        public bool EnableExternalFilter { get; set; } = false;

        /// <summary>
        /// The group id for "Group" filter scope.
        /// </summary>
        [JsonProperty("filterImageReference")]
        public string FilterImageReference { get; set; } = string.Empty;
    }
}
