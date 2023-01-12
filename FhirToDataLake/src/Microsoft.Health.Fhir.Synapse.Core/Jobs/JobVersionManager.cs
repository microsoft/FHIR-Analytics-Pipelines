// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class JobVersionManager
    {
        public const SupportedJobVersion CurrentJobVersion = SupportedJobVersion.V2;

        public const SupportedJobVersion DefaultJobVersion = SupportedJobVersion.V1;

        public const string JobVersionKey = "JobVersion";
    }
}
