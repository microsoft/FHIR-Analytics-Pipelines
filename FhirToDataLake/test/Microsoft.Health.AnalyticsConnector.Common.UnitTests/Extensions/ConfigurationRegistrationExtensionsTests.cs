// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using Microsoft.Health.AnalyticsConnector.Common.Extensions;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Common.UnitTests.Extensions
{
    public class ConfigurationRegistrationExtensionsTests
    {
        private static readonly Dictionary<string, string> TestValidBaseConfiguration = new Dictionary<string, string>
        {
            { "fhirServer:serverUrl", "https://test.fhir.azurehealthcareapis.com" },
            { "dataSource:fhirServer:serverUrl", "https://test.fhir.azurehealthcareapis.com" },
            { "dataLakeStore:storageUrl", "https://test.blob.core.windows.net/" },
            { "job:jobInfoTableName", "jobinfotable" },
            { "job:metadataTableName", "metadatatable" },
            { "job:jobInfoQueueName", "jobinfoqueue" },
            { "job:containerName", "fhir" },
            { "job:queueUrl", "UseDevelopmentStorage=true" },
            { "job:tableUrl", "UseDevelopmentStorage=true" },
            { "job:schedulerCronExpression", "5 * * * * *" },
            { "job:queueType", "FhirToDataLake" },
        };

        public static IEnumerable<object[]> GetInvalidServiceConfigurationVersion()
        {
            yield return new object[] { -1, "ConfigVersion '-1' is not supported." };
            yield return new object[] { 0, "ConfigVersion '0' is not supported." };
            yield return new object[] { 3, "ConfigVersion '3' is not supported." };
            yield return new object[] { "1.0", "ConfigVersion '1.0' is not supported." };
            yield return new object[] { "abc", "ConfigVersion 'abc' is not supported." };
        }

        public static IEnumerable<object[]> GetInvalidServiceConfigurationV1()
        {
            yield return new object[] { "fhirServer:serverUrl", string.Empty, "Fhir server url can not be empty." };
            yield return new object[] { "fhirServer:version", "STU3", "Fhir version Stu3 is not supported." };
            yield return new object[] { "fhirServer:version", "invalidVersion", "Failed to parse data source configuration" };
            yield return new object[] { "job:containerName", string.Empty, "Target azure container name can not be empty." };
            yield return new object[] { "dataLakeStore:storageUrl", string.Empty, "Target azure storage url can not be empty." };
            yield return new object[] { "job:startTime", "invalidDataTime", "Failed to parse job configuration" };
            yield return new object[] { "filter:filterScope", "invalidScope", "Failed to parse filter configuration" };
            yield return new object[] { "schema:schemaImageReference", 12345, "Found the schema image reference but customized schema is disable." };
        }

        public static IEnumerable<object[]> GetInvalidServiceConfigurationV2()
        {
            yield return new object[] { "dataSource:fhirServer:serverUrl", string.Empty, "Fhir server url can not be empty." };
            yield return new object[] { "dataSource:fhirServer:version", "STU3", "Fhir version Stu3 is not supported." };
            yield return new object[] { "dataSource:fhirServer:version", "V1", "Failed to parse data source configuration" };
            yield return new object[] { "job:containerName", string.Empty, "Target azure container name can not be empty." };
            yield return new object[] { "dataLakeStore:storageUrl", string.Empty, "Target azure storage url can not be empty." };
            yield return new object[] { "job:startTime", "invalidDataTime", "Failed to parse job configuration" };
            yield return new object[] { "filter:filterScope", "invalidScope", "Failed to parse filter configuration" };
            yield return new object[] { "schema:schemaImageReference", 12345, "Found the schema image reference but customized schema is disable." };
        }

        public static IEnumerable<object[]> GetInvalidServiceConfigurationForDicom()
        {
            yield return new object[] { "dataSource:dicomServer:serverUrl", string.Empty, "DICOM server url can not be empty." };
            yield return new object[] { "dataSource:dicomServer:apiVersion", "V1_0_Prerelease", "DICOM server API version V1_0_Prerelease is not supported." };
            yield return new object[] { "dataSource:dicomServer:apiVersion", "V2", "Failed to parse data source configuration" };
        }

        [Theory]
        [MemberData(nameof(GetInvalidServiceConfigurationVersion))]
        public void GivenInvalidServiceCollectionConfigurationVersion_WhenValidate_ExceptionShouldBeThrown(string configVersion, string expectedMessageStart)
        {
            Dictionary<string, string> config = new(TestValidBaseConfiguration)
            {
                [ConfigurationConstants.ConfigVersionKey] = configVersion,
            };

            var builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(config);
            var serviceCollection = new ServiceCollection();

            var exception = Assert.Throws<ConfigurationErrorException>(() => serviceCollection.AddConfiguration(builder.Build()));
            Assert.StartsWith(expectedMessageStart, exception.Message);
        }

        [Theory]
        [MemberData(nameof(GetInvalidServiceConfigurationV1))]
        public void GivenInvalidServiceCollectionConfigurationV1_WhenValidate_ExceptionShouldBeThrown(string configKey, string configValue, string expectedMessageStart)
        {
            Dictionary<string, string> config = new (TestValidBaseConfiguration)
            {
                [ConfigurationConstants.ConfigVersionKey] = SupportedConfigVersion.V1.ToString(),
                [configKey] = configValue,
            };

            var builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(config);
            var serviceCollection = new ServiceCollection();

            var exception = Assert.Throws<ConfigurationErrorException>(() => serviceCollection.AddConfiguration(builder.Build()));
            Assert.StartsWith(expectedMessageStart, exception.Message);
        }

        [Theory]
        [MemberData(nameof(GetInvalidServiceConfigurationV2))]
        public void GivenInvalidServiceCollectionConfigurationV2_WhenValidate_ExceptionShouldBeThrown(string configKey, string configValue, string expectedMessageStart)
        {
            Dictionary<string, string> config = new (TestValidBaseConfiguration)
            {
                [ConfigurationConstants.ConfigVersionKey] = SupportedConfigVersion.V2.ToString(),
                [configKey] = configValue,
            };

            var builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(config);
            var serviceCollection = new ServiceCollection();

            var exception = Assert.Throws<ConfigurationErrorException>(() => serviceCollection.AddConfiguration(builder.Build()));
            Assert.StartsWith(expectedMessageStart, exception.Message);
        }

        [Theory]
        [MemberData(nameof(GetInvalidServiceConfigurationForDicom))]
        public void GivenInvalidServiceCollectionConfigurationForDicom_WhenValidate_ExceptionShouldBeThrown(string configKey, string configValue, string expectedMessageStart)
        {
            Dictionary<string, string> config = new (TestValidBaseConfiguration)
            {
                ["dataSource:type"] = "DICOM",
                ["dataSource:dicomServer:serverUrl"] = "https://test.dicom.azurehealthcareapis.com",
                [ConfigurationConstants.ConfigVersionKey] = SupportedConfigVersion.V2.ToString(),
                [configKey] = configValue,
            };

            var builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(config);
            var serviceCollection = new ServiceCollection();

            var exception = Assert.Throws<ConfigurationErrorException>(() => serviceCollection.AddConfiguration(builder.Build()));
            Assert.StartsWith(expectedMessageStart, exception.Message);
        }

        [Fact]
        public void GivenValidServiceCollectionConfigurationV1_WhenValidate_NoExceptionShouldBeThrown()
        {
            var builder = new ConfigurationBuilder();

            // FhirServerConfiguration
            var fhirConfiguration = new Dictionary<string, string>(TestValidBaseConfiguration)
            {
                [ConfigurationConstants.ConfigVersionKey] = SupportedConfigVersion.V1.ToString(),
                ["fhirServer:serverUrl"] = "https://test.fhir.azurehealthcareapis.com",
                ["fhirServer:version"] = "r5",
            };
            fhirConfiguration.Remove("dataSource:fhirServer:serverUrl");

            builder.AddInMemoryCollection(fhirConfiguration);
            IConfigurationRoot config = builder.Build();

            var serviceCollection = new ServiceCollection();
            Exception exception = Record.Exception(() => serviceCollection.AddConfiguration(config));
            Assert.Null(exception);
        }

        [Fact]
        public void GivenValidServiceCollectionConfigurationV2_WhenValidate_NoExceptionShouldBeThrown()
        {
            var builder = new ConfigurationBuilder();

            // DataSourceConfiguration with FHIR Server
            var fhirConfiguration = new Dictionary<string, string>(TestValidBaseConfiguration)
            {
                [ConfigurationConstants.ConfigVersionKey] = SupportedConfigVersion.V2.ToString(),
                ["dataSource:fhirServer:serverUrl"] = "https://test.fhir.azurehealthcareapis.com",
                ["dataSource:fhirServer:version"] = "r5",
            };
            fhirConfiguration.Remove("fhirServer:serverUrl");

            builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(fhirConfiguration);
            IConfigurationRoot config = builder.Build();

            var serviceCollection = new ServiceCollection();
            Exception exception = Record.Exception(() => serviceCollection.AddConfiguration(config));
            Assert.Null(exception);

            // DataSourceConfiguration with DICOM Server
            var dicomConfiguration = new Dictionary<string, string>(TestValidBaseConfiguration)
            {
                [ConfigurationConstants.ConfigVersionKey] = SupportedConfigVersion.V2.ToString(),
                ["dataSource:type"] = "dIcoM",
                ["dataSource:dicomServer:serverUrl"] = "https://test.dicom.azurehealthcareapis.com",
                ["dataSource:dicomServer:apiVersion"] = "v1",
            };
            dicomConfiguration.Remove("fhirServer:serverUrl");

            builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(dicomConfiguration);
            config = builder.Build();

            serviceCollection = new ServiceCollection();
            exception = Record.Exception(() => serviceCollection.AddConfiguration(config));
            Assert.Null(exception);
        }
    }
}
