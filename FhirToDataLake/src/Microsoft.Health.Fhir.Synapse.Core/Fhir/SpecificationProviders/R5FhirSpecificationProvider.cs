// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirR5;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EnsureThat;
using FhirR5::Hl7.Fhir.Model;
using FhirR5::Hl7.Fhir.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using R5FhirModelInfo = FhirR5::Hl7.Fhir.Model.ModelInfo;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders
{
    public class R5FhirSpecificationProvider : IFhirSpecificationProvider
    {
        private readonly IFhirDataClient _dataClient;
        private readonly ILogger<R5FhirSpecificationProvider> _logger;

        private readonly IEnumerable<string> _excludeTypes = new List<string> { FhirConstants.StructureDefinition };

        /// <summary>
        /// Download from https://hl7.org/fhir/5.0.0-snapshot1/compartmentdefinition-patient.json.html
        /// </summary>
        private readonly IEnumerable<string> _compartmentFiles = new List<string> { "Fhir/Data/R5/compartmentdefinition-patient.json" };

        /// <summary>
        /// Download from https://hl7.org/fhir/5.0.0-snapshot1/search-parameters.json, which is defined in https://hl7.org/fhir/5.0.0-snapshot1/searchparameter.html
        /// </summary>
        private readonly string _searchParameterFile = "Fhir/Data/R5/search-parameters.json";

        /// <summary>
        /// The resource types of each compartment type, extracted from _compartmentFiles
        /// </summary>
        private readonly Dictionary<string, HashSet<string>> _compartmentResourceTypesLookup;

        /// <summary>
        /// The FHIR server supported search parameters for each resource type, extracted from FHIR server metadata.
        /// </summary>
        private readonly Dictionary<string, HashSet<string>> _resourceTypeSearchParametersLookup;

        /// <summary>
        /// {resourceType}_{searchParameter} to search parameter id defined by
        /// </summary>
        private readonly Dictionary<string, string> _searchParameterIdLookup;

        public R5FhirSpecificationProvider(
            IFhirDataClient dataClient,
            ILogger<R5FhirSpecificationProvider> logger)
        {
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _compartmentResourceTypesLookup = BuildCompartmentResourceTypesLookup();

            // _searchParameterDefinitionLookup = BuildSearchParameterDefinitionLookup();
            (_resourceTypeSearchParametersLookup, _searchParameterIdLookup) = BuildSearchParametersLookup();
        }

        public IEnumerable<string> GetAllResourceTypes()
        {
            return R5FhirModelInfo.SupportedResources.Except(_excludeTypes);
        }

        public bool IsValidFhirResourceType(string resourceType)
        {
            return R5FhirModelInfo.IsKnownResource(resourceType);
        }

        public IEnumerable<string> GetCompartmentResourceTypes(string compartmentType)
        {
            if (!IsValidCompartmentType(compartmentType))
            {
                _logger.LogError($"The compartment type {compartmentType} isn't a valid compartment type.");
                throw new FhirSpecificationProviderException($"The compartment type {compartmentType} isn't a valid compartment type.");
            }

            if (!_compartmentResourceTypesLookup.ContainsKey(compartmentType))
            {
                _logger.LogError($"The compartment type {compartmentType} isn't supported now.");
                throw new FhirSpecificationProviderException($"The compartment type {compartmentType} isn't supported now.");
            }

            return _compartmentResourceTypesLookup[compartmentType];
        }

        public IEnumerable<string> GetSearchParametersByResourceType(string resourceType)
        {
            if (!IsValidFhirResourceType(resourceType))
            {
                _logger.LogError($"The input {resourceType} isn't a valid resource type.");
                throw new FhirSpecificationProviderException($"The input {resourceType} isn't a valid resource type.");
            }

            if (!_resourceTypeSearchParametersLookup.ContainsKey(resourceType))
            {
                _logger.LogWarning($"There isn't any search parameter defined for resource type {resourceType}.");
                return new HashSet<string>();
            }

            return _resourceTypeSearchParametersLookup[resourceType];
        }

        private bool IsValidCompartmentType(string compartmentType)
        {
            return compartmentType != null && Enum.IsDefined(typeof(CompartmentType), compartmentType);
        }

        private Dictionary<string, HashSet<string>> BuildCompartmentResourceTypesLookup()
        {
            var parser = new FhirJsonParser();
            var compartmentResourceTypesLookup = new Dictionary<string, HashSet<string>>();

            foreach (var compartmentFile in _compartmentFiles)
            {
                string compartmentContext;
                try
                {
                    compartmentContext = File.ReadAllText(compartmentFile);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Read compartment file \"{compartmentFile}\" failed. Reason: {ex.Message}.");
                    throw new FhirSpecificationProviderException($"Read compartment file \"{compartmentFile}\" failed. Reason: {ex.Message}.", ex);
                }

                CompartmentDefinition compartment;

                try
                {
                    compartment = parser.Parse<CompartmentDefinition>(compartmentContext);
                }
                catch (Exception exception)
                {
                    _logger.LogError($"Failed to parse compartment definition from file {compartmentFile}. Reason: {exception.Message}");
                    throw new FhirSpecificationProviderException($"Failed to parse compartment definition from file {compartmentFile}.", exception);
                }

                var compartmentType = compartment.Code?.ToString();
                if (IsValidCompartmentType(compartmentType))
                {
                    var resourceTypes = compartment.Resource?.Where(x => x.Param.Any()).Select(x => x.Code?.ToString()).ToHashSet();
                    if (resourceTypes == null)
                    {
                        _logger.LogWarning($"There is not any resource type defined for compartment type {compartmentType} in file {compartmentFile}");
                    }
                    else
                    {
                        compartmentResourceTypesLookup.Add(compartmentType, resourceTypes);
                        _logger.LogInformation($"There are {resourceTypes.Count} resources type pertained to compartment type {compartmentType}.");
                    }
                }
                else
                {
                    _logger.LogWarning($"The compartment type {compartmentType} in file {compartmentFile} isn't a valid compartment type.");
                }
            }

            return compartmentResourceTypesLookup;
        }

        /// <summary>
        /// Retrieve Fhir server metadata and build _resourceTypeSearchParametersLookup based on it.
        /// </summary>
        private Tuple<Dictionary<string, HashSet<string>>, Dictionary<string, string>> BuildSearchParametersLookup()
        {
            var metadataOptions = new MetadataOptions();

            string metaData;
            try
            {
                metaData = _dataClient.Search(metadataOptions);
            }
            catch (Exception exception)
            {
                _logger.LogError($"Failed to request Fhir server metadata. Reason: {exception.Message}.");
                throw new FhirSpecificationProviderException($"Failed to request Fhir server metadata.", exception);
            }

            var parser = new FhirJsonParser();

            CapabilityStatement capabilityStatement;
            try
            {
                capabilityStatement = parser.Parse<CapabilityStatement>(metaData);
            }
            catch (Exception exception)
            {
                _logger.LogError($"Failed to parse capability statement from FHIR server metadata. Reason: {exception.Message}");
                throw new FhirSpecificationProviderException($"Failed to parse capability statement from FHIR server metadata.", exception);
            }

            var searchParameters = new Dictionary<string, HashSet<string>>();
            var searchParameterIds = new Dictionary<string, string>();

            var rest = capabilityStatement?.Rest;
            if (rest == null || !rest.Any())
            {
                _logger.LogError($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
                throw new FhirSpecificationProviderException($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
            }

            var resources = rest.First().Resource;

            if (resources == null || !resources.Any())
            {
                _logger.LogError($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
                throw new FhirSpecificationProviderException($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
            }

            foreach (var resource in resources)
            {
                var type = resource.Type?.ToString();
                if (!string.IsNullOrEmpty(type))
                {
                    searchParameters[type] = resource.SearchParam.Select(x => x.Name).ToHashSet();
                    resource.SearchParam.ForEach(x => searchParameterIds[SearchParameterKey(type, x.Name)] = x.Definition);
                }
            }

            if (!searchParameters.Any())
            {
                _logger.LogError("There is not any items in the built SearchParametersLookup.");
                throw new FhirSpecificationProviderException("There is not any items in the built SearchParametersLookup.");
            }

            _logger.LogInformation($"Build SearchParametersLookup from fhir server metadata successfully.");

            return new Tuple<Dictionary<string, HashSet<string>>, Dictionary<string, string>>(searchParameters, searchParameterIds);
        }

        private string SearchParameterKey(string resourceType, string searchParameter) => $"{resourceType}_{searchParameter}";
    }
}
