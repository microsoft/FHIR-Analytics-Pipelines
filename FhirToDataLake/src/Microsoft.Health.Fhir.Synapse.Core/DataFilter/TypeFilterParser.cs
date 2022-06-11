// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirStu3;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FhirStu3::Hl7.Fhir.Model;
using FhirStu3::Hl7.Fhir.Serialization;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public static class TypeFilterParser
    {
        public static Common.Models.FhirSearch.DataFilter CreateDataFilter(IFhirSpecificationProvider fhirSpecificationProvider, JobType jobType, string typeString, string filterString)
        {
            var types = ParseType(fhirSpecificationProvider, jobType, typeString);
            var typeFilters = ParseTypeFilter(filterString);

            ValidateTypeFilters(jobType, types, typeFilters);

            return new Common.Models.FhirSearch.DataFilter(types, typeFilters);
        }

        /// <summary>
        /// Parse the type configuration from a string to a list of resource types.
        /// If the type is null, return all resource types for system job and return patient compartment resource types for group job.
        /// </summary>
        /// <param name="jobType">job type.</param>
        /// <param name="typeString">type string.</param>
        /// <returns>a list of resource type.</returns>
        public static List<string> ParseType(IFhirSpecificationProvider fhirSpecificationProvider, JobType jobType, string typeString)
        {
            throw new NotImplementedException();

        }

        /// <summary>
        /// Parses the typeFilter configuration from a string into a list of <see cref="TypeFilter"/> objects.
        /// </summary>
        /// <param name="filterString">The typeFilter parameter input.</param>
        /// <returns>A list of <see cref="TypeFilter"/></returns>
        private static List<TypeFilter> ParseTypeFilter(string filterString)
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

        /// <summary>
        /// Validate:
        /// 1. the types are valid resource types
        /// 2. for group export, the types are patient compartment resource types
        /// 3. the resource type in typefilter is in types
        /// 4. the parameters are supported parameters, search result parameters aren't supported
        /// </summary>
        /// <param name="jobType">job type.</param>
        /// <param name="types">required resource types.</param>
        /// <param name="typeFilters">type filters</param>
        private static void ValidateTypeFilters(JobType jobType, List<string> types, List<TypeFilter> typeFilters)
        {

        }
    }
}
