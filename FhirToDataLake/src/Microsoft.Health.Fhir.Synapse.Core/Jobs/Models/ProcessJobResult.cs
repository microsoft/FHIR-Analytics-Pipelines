// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class ProcessJobResult
    {
        public int SuccessfulResourceCount { get; set; }

        public int FailedResourceCount { get; set; }

        public DateTimeOffset CreateTime { get; set; }

        public DateTimeOffset ProcessStartTime { get; set; }

        public DateTimeOffset ProcessCompleteTime { get; set; }
    }
}
