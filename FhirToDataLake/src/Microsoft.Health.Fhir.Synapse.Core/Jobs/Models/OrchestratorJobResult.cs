// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class OrchestratorJobResult
    {
        public DateTimeOffset CreateTime { get; set; }

        public long SuccessfulResourceCount { get; set; }

        public long FailedResourceCount { get; set; }

        public int CreatedJobCount { get; set; }

        public DateTimeOffset CreatedJobTimestamp { get; set; }

        public ISet<long> RunningJobIds { get; set; } = new HashSet<long>();
    }
}
