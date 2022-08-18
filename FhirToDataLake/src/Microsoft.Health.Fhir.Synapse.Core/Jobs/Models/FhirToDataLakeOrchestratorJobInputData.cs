// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeOrchestratorJobInputData
    {
        public JobType JobType { get; set; }

        public DateTimeOffset? DataStartTime { get; set; }

        public DateTimeOffset DataEndTime { get; set; }
    }
}