// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Api
{
    public class ApiDataSourceTests
    {
        [Fact]
        public void GivenNullDataSourceConfiguration_WhenInitialize_ArgumentNullExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(() => new ApiDataSource(null));
        }

        [Fact]
        public void GivenNullServerUrl_WhenInitialize_ArgumentNullExceptionShouldBeThrown()
        {
            // FHIR Server
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                FhirServer = new FhirServerConfiguration
                {
                    ServerUrl = null,
                },
            };

            Assert.Throws<ArgumentNullException>(() => new ApiDataSource(Options.Create(dataSourceConfiguration)));

            // DICOM Server
            dataSourceConfiguration = new DataSourceConfiguration
            {
                Type = DataSourceType.DICOM,
                DicomServer = new DicomServerConfiguration
                {
                    ServerUrl = null,
                },
            };

            Assert.Throws<ArgumentNullException>(() => new ApiDataSource(Options.Create(dataSourceConfiguration)));
        }

        [Fact]
        public void GivenEmptyServerUrl_WhenInitialize_ArgumentExceptionShouldBeThrown()
        {
            // FHIR Server
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                FhirServer = new FhirServerConfiguration(),
            };

            Assert.Throws<ArgumentException>(() => new ApiDataSource(Options.Create(dataSourceConfiguration)));

            // DICOM Server
            dataSourceConfiguration = new DataSourceConfiguration
            {
                DicomServer = new DicomServerConfiguration(),
            };

            Assert.Throws<ArgumentException>(() => new ApiDataSource(Options.Create(dataSourceConfiguration)));
        }

        [Theory]
        [InlineData("https://example.com", "https://example.com/")]
        [InlineData("https://example.com/", "https://example.com/")]
        public void GivenFhirServerUrl_WhenInitialize_ServerUrlShouldBeEndsWithSlash(string serverUrl, string expectedServerUrl)
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

        [Theory]
        [InlineData("https://example.com", "v1", "https://example.com/v1/")]
        [InlineData("https://example.com/", "v1", "https://example.com/v1/")]
        [InlineData("https://example.com", "v1_0_prerelease", "https://example.com/v1.0-prerelease/")]
        [InlineData("https://example.com/", "v1_0_prerelease", "https://example.com/v1.0-prerelease/")]
        public void GivenDicomServerUrlAndVersion_WhenInitialize_ServerUrlShouldBeEndsWithSlash(string serverUrl, string version, string expectedServerUrl)
        {
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                Type = DataSourceType.DICOM,
                DicomServer = new DicomServerConfiguration
                {
                    ServerUrl = serverUrl,
                    ApiVersion = Enum.Parse<DicomApiVersion>(version, true),
                },
            };

            var dataSource = new ApiDataSource(Options.Create(dataSourceConfiguration));

            Assert.Equal(expectedServerUrl, dataSource.ServerUrl);
        }
    }
}
