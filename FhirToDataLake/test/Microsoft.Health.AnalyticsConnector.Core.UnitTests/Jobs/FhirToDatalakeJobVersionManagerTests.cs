// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.AnalyticsConnector.Core.Jobs;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.Jobs
{
    public class FhirToDatalakeJobVersionManagerTests
    {
        [Fact]
        public void TheDefaultJobVersionShouldBeV1()
        {
            // the default job version should always be V1, as we don't add job version field in V1
            Assert.Equal(JobVersion.V1, FhirToDatalakeJobVersionManager.DefaultJobVersion);
        }

        [Fact]
        public void TheCurrentJobVersionShouldBeV4()
        {
            Assert.Equal(JobVersion.V4, FhirToDatalakeJobVersionManager.CurrentJobVersion);
        }
    }
}
