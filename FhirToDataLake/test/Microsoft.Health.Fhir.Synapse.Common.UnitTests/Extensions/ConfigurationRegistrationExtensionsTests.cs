// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
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
            yield return new object[] { "dataSource:fhirServer:version", "v1", "Failed to parse data source configuration" };
        }

        [Theory]
        [MemberData(nameof(GetInvalidServiceConfiguration))]
        public void GivenInvalidServiceCollectionConfiguration_WhenValidate_ExceptionShouldBeThrown(string configKey, string configValue, string expectedMessageStart)
        {
            Dictionary<string, string> config = new Dictionary<string, string>(TestValidConfiguration);
            config[configKey] = configValue;

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
                ["dataSource:type"] = "DICOM",
                ["dataSource:dicomServer:serverUrl"] = "https://test.dicom.azurehealthcareapis.com",
            };
            dicomConfiguration.Remove("fhirServer:serverUrl");

            builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(dicomConfiguration);
            config = builder.Build();

            serviceCollection = new ServiceCollection();
            exception = Record.Exception(() => serviceCollection.AddConfiguration(config));
            Assert.Null(exception);
        }

        private static void RegistryConfiguration(IServiceCollection services, IConfigurationRoot configuration)
        {
            services.Configure<FhirServerConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.FhirServerConfigurationKey).Bind(options));
            services.Configure<JobConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.JobConfigurationKey).Bind(options));
            services.Configure<FilterConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.FilterConfigurationKey).Bind(options));
            services.Configure<DataLakeStoreConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.DataLakeStoreConfigurationKey).Bind(options));
            services.Configure<ArrowConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.ArrowConfigurationKey).Bind(options));
            services.Configure<SchemaConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.SchemaConfigurationKey).Bind(options));
        }
    }
}
