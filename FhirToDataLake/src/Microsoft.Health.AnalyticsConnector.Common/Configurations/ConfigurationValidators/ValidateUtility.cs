// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using Microsoft.Health.AnalyticsConnector.Common.Models.Jobs;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations.ConfigurationValidators
{
    public static class ValidateUtility
    {
        /// <summary>
        /// Validate FilterConfiguration.
        /// </summary>
        /// <param name="filterConfiguration">FilterConfiguration instance.</param>
        /// <exception cref="ConfigurationErrorException">Throw ConfigurationErrorException if filterConfiguration is invalid.</exception>
        public static void ValidateFilterConfiguration(FilterConfiguration filterConfiguration)
        {
            if (!Enum.IsDefined(typeof(FilterScope), filterConfiguration.FilterScope))
            {
                throw new ConfigurationErrorException(
                    $"Filter Scope '{filterConfiguration.FilterScope}' is not supported.");
            }

            if (filterConfiguration.FilterScope == FilterScope.Group && string.IsNullOrWhiteSpace(filterConfiguration.GroupId))
            {
                throw new ConfigurationErrorException("Group id can not be null, empty or white space for `Group` filter scope.");
            }
        }

        // Validate queue or table name to contain only alphanumeric characters, and not begin with a numeric character.
        // Reference: https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#table-names
        // https://docs.microsoft.com/en-us/rest/api/storageservices/naming-queues-and-metadata#queue-names
        public static void ValidateQueueOrTableName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ConfigurationErrorException("Queue or table name can not be empty.");
            }

            if (!name.All(char.IsLetterOrDigit))
            {
                throw new ConfigurationErrorException("Queue or table name may contain only alphanumeric characters.");
            }

            if (!char.IsLetter(name.First()))
            {
                throw new ConfigurationErrorException("Queue or table name should begin with an alphabet character.");
            }

            // The table/queue name must be from 3 to 63 characters long.
            if (name.Length < 3 || name.Length > 63)
            {
                throw new ConfigurationErrorException("Queue or table name should be from 3 to 63 characters long.");
            }
        }

        /// <summary>
        /// Validate the image reference.
        /// Image reference pattern: <registry>/<name>@<digest> or <registry>/<name>:<tag>
        /// E.g. testacr.azurecr.io/templatetest@sha256:412ea84f1bb1a9d98345efb7b427ba89616ec29ac332d543eff9a2161ca12a58
        /// </summary>
        /// <param name="imageReference">Image reference</param>
        /// <exception cref="ConfigurationErrorException">Throw ConfigurationErrorException if imageReference is invalid.</exception>
        public static void ValidateImageReference(string imageReference)
        {
            int registryDelimiterPosition = imageReference.IndexOf(ConfigurationConstants.ImageRegistryDelimiter, StringComparison.InvariantCultureIgnoreCase);
            if (registryDelimiterPosition <= 0 || registryDelimiterPosition == imageReference.Length - 1)
            {
                throw new ConfigurationErrorException("Customized schema image format is invalid: registry server is missing.");
            }

            imageReference = imageReference[(registryDelimiterPosition + 1) ..];
            string imageName = imageReference;
            if (imageReference.Contains(ConfigurationConstants.ImageDigestDelimiter, StringComparison.OrdinalIgnoreCase))
            {
                Tuple<string, string> imageMeta = ParseImageMeta(imageReference, ConfigurationConstants.ImageDigestDelimiter);
                if (string.IsNullOrEmpty(imageMeta.Item1) || string.IsNullOrEmpty(imageMeta.Item2))
                {
                    throw new ConfigurationErrorException("Customized schema image format is invalid: digest is missing.");
                }

                imageName = imageMeta.Item1;
            }
            else if (imageReference.Contains(ConfigurationConstants.ImageTagDelimiter, StringComparison.OrdinalIgnoreCase))
            {
                Tuple<string, string> imageMeta = ParseImageMeta(imageReference, ConfigurationConstants.ImageTagDelimiter);
                if (string.IsNullOrEmpty(imageMeta.Item1) || string.IsNullOrEmpty(imageMeta.Item2))
                {
                    throw new ConfigurationErrorException("Customized schema image reference format is invalid: tag is missing.");
                }

                imageName = imageMeta.Item1;
            }

            ValidateImageName(imageName);
        }

        private static Tuple<string, string> ParseImageMeta(string input, char delimiter)
        {
            int index = input.IndexOf(delimiter, StringComparison.InvariantCultureIgnoreCase);
            return new Tuple<string, string>(input[..index], input[(index + 1) ..]);
        }

        private static void ValidateImageName(string imageName)
        {
            if (!ConfigurationConstants.ImageNameRegex.IsMatch(imageName))
            {
                throw new ConfigurationErrorException(
                    $"Customized schema image name is invalid. Image name should contains lowercase letters, digits and separators. The valid format is {ConfigurationConstants.ImageNameRegex}");
            }
        }
    }
}
