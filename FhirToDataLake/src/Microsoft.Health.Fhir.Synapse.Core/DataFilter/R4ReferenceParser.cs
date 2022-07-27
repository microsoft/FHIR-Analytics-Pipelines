// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirR4;

using System;
using System.Linq;
using System.Text.RegularExpressions;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;

using R4FhirModelInfo = FhirR4::Hl7.Fhir.Model.ModelInfo;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public class R4ReferenceParser : IReferenceParser
    {
        private readonly ILogger<R4ReferenceParser> _logger;
        private readonly IFhirApiDataSource _dataSource;

        private const string ResourceTypeCapture = "resourceType";
        private const string ResourceIdCapture = "resourceId";
        private static readonly string[] SupportedSchemes = { Uri.UriSchemeHttps, Uri.UriSchemeHttp };
        private static readonly string ResourceTypesPattern = string.Join('|', R4FhirModelInfo.SupportedResources);
        private static readonly string ReferenceCaptureRegexPattern = $@"(?<{ResourceTypeCapture}>{ResourceTypesPattern})\/(?<{ResourceIdCapture}>[A-Za-z0-9\-\.]{{1,64}})(\/_history\/[A-Za-z0-9\-\.]{{1,64}})?";

        private static readonly Regex ReferenceRegex = new(
            ReferenceCaptureRegexPattern,
            RegexOptions.Singleline | RegexOptions.Compiled | RegexOptions.ExplicitCapture);

        public R4ReferenceParser(
            IFhirApiDataSource dataSource,
            ILogger<R4ReferenceParser> logger)
        {
            _dataSource = EnsureArg.IsNotNull(dataSource, nameof(dataSource));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public FhirReference Parse(string reference)
        {
            if (string.IsNullOrWhiteSpace(reference))
            {
                _logger.LogError("The reference string is null or white space.");
                throw new ReferenceParseException("The reference string is null or white space.");
            }

            var match = ReferenceRegex.Match(reference);

            if (match.Success)
            {
                var resourceTypeInString = match.Groups[ResourceTypeCapture].Value;

                if (R4FhirModelInfo.IsKnownResource(resourceTypeInString))
                {
                    var resourceId = match.Groups[ResourceIdCapture].Value;

                    var resourceTypeStartIndex = match.Groups[ResourceTypeCapture].Index;

                    if (resourceTypeStartIndex == 0)
                    {
                        // This is relative URL.
                        return new FhirReference(resourceTypeInString, resourceId);
                    }

                    try
                    {
                        var baseUri = new Uri(reference.Substring(0, resourceTypeStartIndex), UriKind.RelativeOrAbsolute);
                        if (baseUri.IsAbsoluteUri)
                        {
                            if (baseUri.AbsoluteUri == _dataSource.FhirServerUrl)
                            {
                                // This is an absolute URL pointing to an internal resource.
                                return new FhirReference(resourceTypeInString, resourceId);
                            }

                            if (SupportedSchemes.Contains(baseUri.Scheme, StringComparer.OrdinalIgnoreCase))
                            {
                                // This is an absolute URL pointing to an external resource.
                                _logger.LogError($"The reference {reference} is an absolute URL pointing to an external resource.");
                                throw new ReferenceParseException($"The reference {reference} is an absolute URL pointing to an external resource.");
                            }
                        }
                    }
                    catch (UriFormatException ex)
                    {
                        // The reference is not a relative reference but is not a valid absolute reference either.
                        _logger.LogError($"The reference {reference} is not a relative reference but is not a valid absolute reference either. UriFormatException: {ex}.");
                        throw new ReferenceParseException($"The reference {reference} is not a relative reference but is not a valid absolute reference either.", ex);
                    }
                }
                else
                {
                    _logger.LogError($"The resource type {resourceTypeInString} in reference {reference} isn't a known resource type.");
                    throw new ReferenceParseException($"The resource type {resourceTypeInString} in reference {reference} isn't a known resource type.");
                }
            }

            _logger.LogError($"Fail to parse reference {reference}.");
            throw new ReferenceParseException($"Fail to parse reference {reference}.");
        }
    }
}
