// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Common.UnitTests.Extensions
{
    public class IServiceCollectionExtensionsTests
    {
        private static readonly Dictionary<string, string> TestValidConfiguration = new Dictionary<string, string>
        {
            { "fhirServer:serverUrl", "https://test.fhir.azurehealthcareapis.com" },
            { "dataLakeStore:storageUrl", "https://test.blob.core.windows.net/" },
            { "job:containerName", "fhir" },
        };

        public static IEnumerable<object[]> GetInvalidServiceConfiguration()
        {
            yield return new object[] { "fhirServer:serverUrl", string.Empty };
            yield return new object[] { "job:containerName", string.Empty };
            yield return new object[] { "dataLakeStore:storageUrl", string.Empty };
            yield return new object[] { "fhirServer:version", "invalidVersion" };
            yield return new object[] { "job:startTime", "invalidDataTime" };
            yield return new object[] { "filter:filterScope", "invalidScope" };
            yield return new object[] { "schema:schemaImageReference", 12345 };
        }

        public static IEnumerable<object[]> GetInvalidImageReference()
        {
            yield return new object[] { "testacr.azurecr.io@v1" };
            yield return new object[] { "testacr.azurecr.io:templateset:v1" };
            yield return new object[] { "testacr.azurecr.io_v1" };
            yield return new object[] { "testacr.azurecr.io:v1" };
            yield return new object[] { "testacr.azurecr.io/" };
            yield return new object[] { "/testacr.azurecr.io" };
            yield return new object[] { "testacr.azurecr.io/name:" };
            yield return new object[] { "testacr.azurecr.io/:tag" };
            yield return new object[] { "testacr.azurecr.io/name@" };
            yield return new object[] { "testacr.azurecr.io/INVALID" };
            yield return new object[] { "testacr.azurecr.io/invalid_" };
            yield return new object[] { "testacr.azurecr.io/in*valid" };
            yield return new object[] { "testacr.azurecr.io/org/org/in*valid" };
            yield return new object[] { "testacr.azurecr.io/invalid____set" };
            yield return new object[] { "testacr.azurecr.io/invalid....set" };
            yield return new object[] { "testacr.azurecr.io/invalid._set" };
            yield return new object[] { "testacr.azurecr.io/_invalid" };

            // Invalid case sensitive
            yield return new object[] { "testacr.azurecr.io/Templateset:v1" };
            yield return new object[] { "testacr.azurecr.io/TEMPLATESET:v1" };
        }

        public static IEnumerable<object[]> GetValidImageReference()
        {
            yield return new object[] { "testacr.azurecr.io/templateset:v1" };
            yield return new object[] { "testacr.azurecr.io/templateset@sha256:e6dcff9eaf7604aa7a855e52b2cda22c5cfc5cadaa035892557c4ff19630b612" };
            yield return new object[] { "testacr.azurecr.io/templateset" };
            yield return new object[] { "testacr.azurecr.io/org/templateset" };
            yield return new object[] { "testacr.azurecr.io/org/template-set" };
            yield return new object[] { "testacr.azurecr.io/org/template.set" };
            yield return new object[] { "testacr.azurecr.io/org/template__set" };
            yield return new object[] { "testacr.azurecr.io/org/template-----set" };
            yield return new object[] { "testacr.azurecr.io/org/template-set_set.set" };
            yield return new object[] { "testacr.azurecr.io/templateset:V1" };

            // Valid case sensitive
            yield return new object[] { "Testacr.azurecr.io/templateset:v1" };
            yield return new object[] { "TESTACR.azurecr.io/templateset:v1" };
            yield return new object[] { "testacr.Azurecr.io/templateset:v1" };
            yield return new object[] { "testacr.azurecr.IO/templateset:v1" };
            yield return new object[] { "testacr.azurecr.io/templateset:V1" };
        }

        [Theory]
        [MemberData(nameof(GetInvalidServiceConfiguration))]
        public void GivenInvalidServiceCollectionConfiguration_WhenValidate_ExceptionShouldBeThrown(string configKey, string configValue)
        {
            var config = new Dictionary<string, string>(TestValidConfiguration);
            config[configKey] = configValue;

            var builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(config);
            var serviceCollection = new ServiceCollection();
            RegistryConfiguration(serviceCollection, builder.Build());

            Assert.Throws<ConfigurationErrorException>(() => serviceCollection.ValidateConfiguration());
        }

        [Fact]
        public void GivenValidServiceCollectionConfiguration_WhenValidate_NoExceptionShouldBeThrown()
        {
            var builder = new ConfigurationBuilder();
            builder.AddInMemoryCollection(TestValidConfiguration);
            var config = builder.Build();

            var serviceCollection = new ServiceCollection();
            RegistryConfiguration(serviceCollection, config);

            var exception = Record.Exception(() => serviceCollection.ValidateConfiguration());
            Assert.Null(exception);
        }

        [Fact]
        public void GivenInvalidFilterConfiguration_WhenValidate_ExceptionShouldBeThrown()
        {
            var config = new FilterConfiguration()
            {
                FilterScope = FilterScope.Group,
                GroupId = string.Empty,
            };

            Assert.Throws<ConfigurationErrorException>(() => IServiceCollectionExtensions.ValidateFilterConfiguration(config));
        }

        [Theory]
        [MemberData(nameof(GetInvalidImageReference))]
        public void GivenInvalidImageReference_WhenValidate_ExceptionShouldBeThrown(string imageReference)
        {
            Assert.Throws<ConfigurationErrorException>(() => IServiceCollectionExtensions.ValidateImageReference(imageReference));
        }

        [Theory]
        [MemberData(nameof(GetValidImageReference))]
        public void GivenValidImageReference_WhenValidate_NoExceptionShouldBeThrown(string imageReference)
        {
            var exception = Record.Exception(() => IServiceCollectionExtensions.ValidateImageReference(imageReference));
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
            services.Configure<JobSchedulerConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.SchedulerConfigurationKey).Bind(options));
            services.Configure<SchemaConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.SchemaConfigurationKey).Bind(options));
        }
    }
}
