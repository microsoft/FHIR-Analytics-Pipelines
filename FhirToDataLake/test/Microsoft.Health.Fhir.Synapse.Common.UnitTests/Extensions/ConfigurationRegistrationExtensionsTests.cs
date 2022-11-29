// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Common.UnitTests.Extensions
{
    public class ConfigurationRegistrationExtensionsTests
    {
        private static readonly Dictionary<string, string> TestValidConfiguration = new Dictionary<string, string>
        {
            { "fhirServer:serverUrl", "https://test.fhir.azurehealthcareapis.com" },
            { "dataLakeStore:storageUrl", "https://test.blob.core.windows.net/" },
            { "job:jobInfoTableName", "jobinfotable" },
            { "job:metadataTableName", "metadatatable" },
            { "job:jobInfoQueueName", "jobinfoqueue" },
            { "job:containerName", "fhir" },
            { "job:queueUrl", "UseDevelopmentStorage=true" },
            { "job:tableUrl", "UseDevelopmentStorage=true" },
            { "job:schedulerCronExpression", "5 * * * * *" },
            { "job:queueType", "FhirToDataLake" },
            { "configVersion", "1" },
        };

        public static IEnumerable<object[]> GetInvalidServiceConfiguration()
        {
            yield return new object[] { "fhirServer:serverUrl", string.Empty, "Fhir server url can not be empty." };
            yield return new object[] { "fhirServer:version", "invalidVersion", "Failed to parse data source configuration" };
            yield return new object[] { "fhirServer:version", "STU3", "Fhir version Stu3 is not supported." };
            yield return new object[] { "job:containerName", string.Empty, "Target azure container name can not be empty." };
            yield return new object[] { "dataLakeStore:storageUrl", string.Empty, "Target azure storage url can not be empty." };
            yield return new object[] { "job:startTime", "invalidDataTime", "Failed to parse job configuration" };
            yield return new object[] { "filter:filterScope", "invalidScope", "Failed to parse filter configuration" };
            yield return new object[] { "schema:schemaImageReference", 12345, "Found the schema image reference but customized schema is disable." };
            yield return new object[] { "configVersion", -1, "ConfigVersion '-1' is not supported." };
            yield return new object[] { "configVersion", 0, "ConfigVersion '0' is not supported." };
            yield return new object[] { "configVersion", 2, "ConfigVersion '2' is not supported." };
            yield return new object[] { "configVersion", "1.0", "ConfigVersion '1.0' is not supported." };
            yield return new object[] { "configVersion", "abc", "ConfigVersion 'abc' is not supported." };
            yield return new object[] { "dataSource:fhirServer:serverUrl", string.Empty, "Fhir server url can not be empty." };
            yield return new object[] { "dataSource:fhirServer:version", "V1", "Failed to parse data source configuration" };
        }

        public static IEnumerable<object[]> GetInvalidServiceConfigurationForDicom()
        {
            yield return new object[] { "dataSource:dicomServer:serverUrl", string.Empty, "DICOM server url can not be empty." };
            yield return new object[] { "dataSource:dicomServer:apiVersion", "V2", "Failed to parse data source configuration" };
        }

        [Theory]
        [MemberData(nameof(GetInvalidServiceConfiguration))]
        public void GivenInvalidServiceCollectionConfiguration_WhenValidate_ExceptionShouldBeThrown(string configKey, string configValue, string expectedMessageStart)
        {
            Dictionary<string, string> config = new (TestValidConfiguration)
            {
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
            Dictionary<string, string> config = new (TestValidConfiguration)
            {
                [configKey] = configValue,
                ["dataSource:type"] = "DICOM",
            };

            var builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(config);
            var serviceCollection = new ServiceCollection();

            var exception = Assert.Throws<ConfigurationErrorException>(() => serviceCollection.AddConfiguration(builder.Build()));
            Assert.StartsWith(expectedMessageStart, exception.Message);
        }

        [Fact]
        public void GivenValidServiceCollectionConfiguration_WhenValidate_NoExceptionShouldBeThrown()
        {
            // FhirServerConfiguration
            var builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(TestValidConfiguration);
            IConfigurationRoot config = builder.Build();

            var serviceCollection = new ServiceCollection();
            Exception exception = Record.Exception(() => serviceCollection.AddConfiguration(config));
            Assert.Null(exception);

            // DataSourceConfiguration with FHIR Server
            var fhirConfiguration = new Dictionary<string, string>(TestValidConfiguration)
            {
                ["dataSource:fhirServer:serverUrl"] = "https://test.fhir.azurehealthcareapis.com",
                ["dataSource:fhirServer:version"] = "r5",
            };
            fhirConfiguration.Remove("fhirServer:serverUrl");

            builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(fhirConfiguration);
            config = builder.Build();

            serviceCollection = new ServiceCollection();
            exception = Record.Exception(() => serviceCollection.AddConfiguration(config));
            Assert.Null(exception);

            // DataSourceConfiguration with DICOM Server
            var dicomConfiguration = new Dictionary<string, string>(TestValidConfiguration)
            {
                ["dataSource:type"] = "dIcoM",
                ["dataSource:dicomServer:serverUrl"] = "https://test.dicom.azurehealthcareapis.com",
                ["dataSource:dicomServer:version"] = "v1_0_pRereLease",
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
