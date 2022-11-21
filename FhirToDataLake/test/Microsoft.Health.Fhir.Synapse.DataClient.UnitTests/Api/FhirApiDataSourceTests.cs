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
            Assert.Throws<ArgumentNullException>(() => new ApiDataSource(null));
        }

        [Fact]
        public void GivenNullServerUrl_WhenInitialize_ArgumentNullExceptionShouldBeThrown()
        {
            // ServerUrl is string.empty
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                FhirServer = new FhirServerConfiguration
                {
                    ServerUrl = null,
                },
            };

            Assert.Throws<ArgumentNullException>(() => new ApiDataSource(Options.Create(dataSourceConfiguration)));
        }

        [Fact]
        public void GivenEmptyServerUrl_WhenInitialize_ArgumentExceptionShouldBeThrown()
        {
            // ServerUrl is string.empty
            var dataSourceConfiguration = new DataSourceConfiguration();
            Assert.Throws<ArgumentException>(() => new ApiDataSource(Options.Create(dataSourceConfiguration)));

            dataSourceConfiguration.FhirServer.ServerUrl = string.Empty;
            Assert.Throws<ArgumentException>(() => new ApiDataSource(Options.Create(dataSourceConfiguration)));
        }

        [Theory]
        [InlineData("https://example.com", "https://example.com/")]
        [InlineData("https://example.com/", "https://example.com/")]
        public void GivenServerUrl_WhenInitialize_ServerUrlShouldBeEndsWithSlash(string serverUrl, string expectedServerUrl)
        {
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                FhirServer = new FhirServerConfiguration
                {
                    ServerUrl = serverUrl,
                },
            };

            var dataSource = new ApiDataSource(Options.Create(dataSourceConfiguration));

            Assert.Equal(expectedServerUrl, dataSource.ServerUrl);
        }
    }
}
