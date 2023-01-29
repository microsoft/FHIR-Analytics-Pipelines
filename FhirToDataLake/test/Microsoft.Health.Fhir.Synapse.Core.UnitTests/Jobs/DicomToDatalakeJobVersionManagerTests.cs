// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class DicomToDatalakeJobVersionManagerTests
    {
        [Fact]
        public void TheDefaultJobVersionShouldBeV1()
        {
            // the default job version should always be V1, as we don't add job version field in V1
            Assert.Equal(JobVersion.V1, DicomToDatalakeJobVersionManager.DefaultJobVersion);
        }

        [Fact]
        public void TheCurrentJobVersionShouldBeV3()
        {
            Assert.Equal(JobVersion.V1, DicomToDatalakeJobVersionManager.CurrentJobVersion);
        }
    }
}