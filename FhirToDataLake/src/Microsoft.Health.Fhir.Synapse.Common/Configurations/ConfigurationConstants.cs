// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Text.RegularExpressions;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public static class ConfigurationConstants
    {
        public const string JobConfigurationKey = "job";

        public const string FilterConfigurationKey = "filter";

        public const string FhirServerConfigurationKey = "fhirServer";

        public const string DataLakeStoreConfigurationKey = "dataLakeStore";

        public const string ArrowConfigurationKey = "arrow";

        public const string SchemaConfigurationKey = "schema";

        public const string SchedulerConfigurationKey = "scheduler";

        public const string HealthCheckConfigurationKey = "healthCheck";

        public const char ImageDigestDelimiter = '@';
        public const char ImageTagDelimiter = ':';
        public const char ImageRegistryDelimiter = '/';

        // Reference docker's image name format: https://docs.docker.com/engine/reference/commandline/tag/#extended-description
        public static readonly Regex ImageNameRegex = new Regex(@"^[a-z0-9]+(([_\.]|_{2}|\-+)[a-z0-9]+)*(\/[a-z0-9]+(([_\.]|_{2}|\-+)[a-z0-9]+)*)*$");
    }
}
