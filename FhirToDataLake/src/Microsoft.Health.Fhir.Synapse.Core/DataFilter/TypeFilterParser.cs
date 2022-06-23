// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirR4;

using System;
using System.Collections.Generic;
using System.Linq;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public class TypeFilterParser : ITypeFilterParser
    {
        private readonly string _patientString = "Patient";

        private readonly ILogger<TypeFilterParser> _logger;
        private readonly IFhirSpecificationProvider _fhirSpecificationProvider;

        public TypeFilterParser(
            IFhirSpecificationProvider fhirSpecificationProvider,
            ILogger<TypeFilterParser> logger)
        {
            EnsureArg.IsNotNull(fhirSpecificationProvider, nameof(fhirSpecificationProvider));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _fhirSpecificationProvider = fhirSpecificationProvider;
            _logger = logger;
        }

        public Common.Models.FhirSearch.DataFilter CreateDataFilter(JobType jobType, string typeString, string filterString)
        {
            var requiredTypes = ParseType(jobType, typeString).ToHashSet();
            var typeFilters = ParseTypeFilter(filterString).ToList();

            foreach (var typeFilter in typeFilters)
            {
                // The resource type in typeFilter should be in the required types.
                if (!requiredTypes.Contains(typeFilter.ResourceType))
                {
                    _logger.LogError($"The required resource type {typeFilter.ResourceType} in typeFilter isn't in the type parameter {typeString}.");
                    throw new ConfigurationErrorException($"The required resource type {typeFilter.ResourceType} in typeFilter isn't in the type parameter {typeString}.");
                }

                // Validate search parameters
                var supportedSearchParam = _fhirSpecificationProvider.GetSearchParametersByResourceType(typeFilter.ResourceType).ToHashSet();
                foreach (var (param, value_) in typeFilter.Parameters)
                {
                    var splitParams = param.Split(new char[] { ':', '.' });
                    var paramName = splitParams[0];

                    // the parameter name should be the supported parameter of this resource type.
                    if (paramName != "_has" && !supportedSearchParam.Contains(paramName))
                    {
                        _logger.LogError($"The search parameter {paramName} isn't supported.");
                        throw new ConfigurationErrorException(
                            $"The search parameter {paramName} isn't supported.");
                    }

                    // TODO: validate modifier/reverse chaining/chained

                    // Reverse Chaining
                    // GET [base]/Patient?_has:Observation:patient:code=1234-5
                    // Chained parameters
                    // GET [base]/DiagnosticReport?subject:Patient.name=peter
                }
            }

            return new Common.Models.FhirSearch.DataFilter(requiredTypes.ToList(), typeFilters);
        }

        /// <summary>
        /// Parse the type configuration from a string to a list of resource types.
        /// If the type is null, return all resource types for system job and return patient compartment resource types for group job.
        /// If the type isn't supported, will throw an exception.
        /// </summary>
        /// <param name="jobType">job type.</param>
        /// <param name="typeString">type string.</param>
        /// <returns>a list of resource type.</returns>
        private IEnumerable<string> ParseType(JobType jobType, string typeString)
        {
            HashSet<string> supportedResourceTypes = jobType switch
            {
                JobType.System => _fhirSpecificationProvider.GetAllResourceTypes().ToHashSet(),
                JobType.Group => _fhirSpecificationProvider.GetCompartmentResourceTypes(_patientString).ToHashSet(),
                _ => throw new ConfigurationErrorException($"The JobType {jobType} isn't supported now.")
            };

            if (string.IsNullOrEmpty(typeString))
            {
                return supportedResourceTypes;
            }

            var types = typeString?.Split(',').ToHashSet();

            foreach (var type in types.Where(type => !_fhirSpecificationProvider.IsValidFhirResourceType(type)))
            {
                throw new ConfigurationErrorException($"The required resource type {type} isn't a valid resource type.");
            }

            foreach (var type in types.Where(type => !supportedResourceTypes.Contains(type)))
            {
                throw new ConfigurationErrorException(
                    $"The required resource type {type} isn't supported for scope {jobType}");
            }

            return types;
        }

        /// <summary>
        /// Parses the typeFilter configuration from a string into a list of <see cref="TypeFilter"/> objects.
        /// </summary>
        /// <param name="filterString">The typeFilter parameter input.</param>
        /// <returns>A list of <see cref="TypeFilter"/></returns>
        private IEnumerable<TypeFilter> ParseTypeFilter(string filterString)
        {
            var filters = new List<TypeFilter>();

            if (!string.IsNullOrWhiteSpace(filterString))
            {
                var filterArray = filterString.Split(",");
                foreach (string filter in filterArray)
                {
                    var parameterIndex = filter.IndexOf("?", StringComparison.Ordinal);

                    if (parameterIndex < 0 || parameterIndex == filter.Length - 1)
                    {
                        throw new ConfigurationErrorException(
                            $"The typeFilter segment '{filter}' could not be parsed.");
                    }

                    var filterType = filter.Substring(0, parameterIndex);

                    var filterParameters = filter.Substring(parameterIndex + 1).Split("&");
                    var parameterTupleList = new List<Tuple<string, string>>();

                    foreach (string parameter in filterParameters)
                    {
                        var keyValue = parameter.Split("=");

                        if (keyValue.Length != 2)
                        {
                            throw new ConfigurationErrorException(
                                $"The typeFilter segment '{filter}' could not be parsed.");
                        }

                        parameterTupleList.Add(new Tuple<string, string>(keyValue[0], keyValue[1]));
                    }

                    filters.Add(new TypeFilter(filterType, parameterTupleList));
                }
            }

            return filters;
        }
    }
}
