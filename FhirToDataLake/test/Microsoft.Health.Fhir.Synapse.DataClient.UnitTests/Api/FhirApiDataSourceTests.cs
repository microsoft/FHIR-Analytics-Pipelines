// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Api
{
    public class FhirApiDataSourceTests
    {
        [Fact]
        public void GivenNullFhirServerConfiguration_WhenInitialize_ArgumentNullExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(() => new FhirApiDataSource(null));
        }

        [Fact]
        public void GivenNullServerUrl_WhenInitialize_ArgumentNullExceptionShouldBeThrown()
        {
            // ServerUrl is string.empty
            FhirServerConfiguration fhirServerConfig = new FhirServerConfiguration
            {
                ServerUrl = null,
            };

            Assert.Throws<ArgumentNullException>(() => new FhirApiDataSource(Options.Create(fhirServerConfig)));
        }

        [Fact]
        public void GivenEmptyServerUrl_WhenInitialize_ArgumentExceptionShouldBeThrown()
        {
            // ServerUrl is string.empty
            FhirServerConfiguration fhirServerConfig = new FhirServerConfiguration();
            Assert.Throws<ArgumentException>(() => new FhirApiDataSource(Options.Create(fhirServerConfig)));

            fhirServerConfig.ServerUrl = string.Empty;
            Assert.Throws<ArgumentException>(() => new FhirApiDataSource(Options.Create(fhirServerConfig)));
        }

        [Theory]
        [InlineData("https://example.com", "https://example.com/")]
        [InlineData("https://example.com/", "https://example.com/")]
        public void GivenServerUrl_WhenInitialize_ServerUrlShouldBeEndsWithSlash(string serverUrl, string expectedServerUrl)
        {
            FhirServerConfiguration fhirServerConfig = new FhirServerConfiguration
            {
                ServerUrl = serverUrl,
            };
            FhirApiDataSource dataSource = new FhirApiDataSource(Options.Create(fhirServerConfig));

            Assert.Equal(expectedServerUrl, dataSource.FhirServerUrl);
        }
    }
}
