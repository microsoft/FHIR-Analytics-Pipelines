// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class JobVersionManagerTests
    {
        [Fact]
        public void TheDefaultJobVersionShouldBeV1()
        {
            // the default job version should always be V1, as we don't add job version field in V1
            Assert.Equal(SupportedJobVersion.V1, JobVersionManager.DefaultJobVersion);
        }

        [Fact]
        public void TheCurrentJobVersionShouldBeV2()
        {
            // the current job version V2, be careful to update the current job version. If the current job version is updated, need to consider version compatibility issues
            Assert.Equal(SupportedJobVersion.V2, JobVersionManager.CurrentJobVersion);
        }
    }
}
