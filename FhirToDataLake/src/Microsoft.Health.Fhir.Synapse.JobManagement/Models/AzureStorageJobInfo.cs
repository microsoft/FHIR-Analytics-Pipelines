// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.Models
{
    public class AzureStorageJobInfo : JobInfo
    {
        public long HeartbeatTimeoutSec { get; set; }

        public virtual string JobIdentifier()
        {
            return Definition.ComputeHash();
        }
    }
}