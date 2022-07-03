// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------
extern alias FhirR4;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EnsureThat;
using FhirR4::Hl7.Fhir.Model;
using FhirR4::Hl7.Fhir.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using R4FhirModelInfo = FhirR4::Hl7.Fhir.Model.ModelInfo;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir
{
    extern alias FhirStu3;

    public class R4FhirSpecificationProvider : IFhirSpecificationProvider
    {
        private readonly IFhirDataClient _dataClient;
        private readonly ILogger<R4FhirSpecificationProvider> _logger;

        private readonly IEnumerable<string> _excludeTypes = new List<string> { "StructureDefinition" };

        /// <summary>
        /// Download from http://hl7.org/fhir/R4/compartmentdefinition-patient.json
        /// </summary>
        private readonly IEnumerable<string> _compartmentFiles = new List<string> { "Fhir/Data/R4/compartmentdefinition-patient.json" };

        /// <summary>
        /// Download from http://hl7.org/fhir/R4/search-parameters.json, which is defined in http://hl7.org/fhir/R4/searchparameter-registry.html
        /// </summary>
        private readonly string _searchParameterFile = "Fhir/Data/R4/search-parameters.json";

        private readonly Dictionary<string, HashSet<string>> _compartmentResourceTypesLookup;

        /// <summary>
        /// The FHIR server supported search parameters for each resource type.
        /// </summary>
        private readonly Dictionary<string, HashSet<string>> _resourceTypeSearchParametersLookup;

        /// <summary>
        /// {resourceType}_{searchParameter} to search parameter id
        /// </summary>
        private readonly Dictionary<string, string> _searchParameterIdLookup;

        /// <summary>
        /// search parameter id to search parameter definition
        /// </summary>
        private readonly Dictionary<string, SearchParameter> _searchParameterDefinitionLookup;

        public R4FhirSpecificationProvider(
            IFhirDataClient dataClient,
            ILogger<R4FhirSpecificationProvider> logger)
        {
            EnsureArg.IsNotNull(dataClient, nameof(dataClient));

            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataClient = dataClient;
            _logger = logger;

            _compartmentResourceTypesLookup = BuildCompartmentResourceTypesLookup();

            _searchParameterDefinitionLookup = BuildSearchParameterDefinitionLookup();

            (_resourceTypeSearchParametersLookup, _searchParameterIdLookup) = BuildSearchParametersLookup();
        }

        public IEnumerable<string> GetAllResourceTypes()
        {
            return R4FhirModelInfo.SupportedResources.Except(_excludeTypes);
        }

        public bool IsValidFhirResourceType(string resourceType)
        {
            return R4FhirModelInfo.IsKnownResource(resourceType);
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
                string compartmentContext = null;
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
                    _logger.LogError($"Failed to parse compartment definition from file {compartmentFile}.");
                    throw new FhirSpecificationProviderException($"Failed to parse compartment definition from file {compartmentFile}.", exception);
                }

                var compartmentType = compartment.Code;
                if (compartmentType != null && IsValidCompartmentType(compartmentType.ToString()))
                {
                    var resourceTypes = compartment.Resource.Where(x => x.Param.Any()).Select(x => x.Code?.ToString()).ToHashSet();
                    compartmentResourceTypesLookup.Add(compartmentType?.ToString(), resourceTypes);
                }
            }

            return compartmentResourceTypesLookup;
        }

        private Tuple<Dictionary<string, HashSet<string>>, Dictionary<string, string>> BuildSearchParametersLookup()
        {
            var metadataOptions = new MetadataOptions();
            string metaData = _dataClient.SearchAsync(metadataOptions).Result;
            var parser = new FhirJsonParser();

            CapabilityStatement capabilityStatement;
            try
            {
                capabilityStatement = parser.Parse<CapabilityStatement>(metaData);
            }
            catch (Exception exception)
            {
                _logger.LogError($"Failed to parse capability statement from FHIR server metadata.");
                throw new FhirSpecificationProviderException($"Failed to parse capability statement from FHIR server metadata.", exception);
            }

            var searchParameters = new Dictionary<string, HashSet<string>>();
            var searchParameterIds = new Dictionary<string, string>();

            var resources = capabilityStatement?.Rest?.First().Resource;

            if (resources == null)
            {
                _logger.LogWarning($"Build a empty search parameters lookup: the resource is null.");
            }
            else
            {
                foreach (var resource in resources)
                {
                    var type = resource.Type?.ToString();
                    if (!string.IsNullOrEmpty(type))
                    {
                        searchParameters[type] = resource.SearchParam.Select(x => x.Name).ToHashSet();
                        resource.SearchParam.ForEach(x => searchParameterIds[SearchParameterKey(type, x.Name)] = x.Definition);
                    }
                }
            }

            return new Tuple<Dictionary<string, HashSet<string>>, Dictionary<string, string>>(searchParameters, searchParameterIds);
        }

        /// <summary>
        /// build SearchParameterDefinitionLookup, search parameter url to searchParameter object
        /// </summary>
        /// <returns>search parameter url to searchParameter object dictionary</returns>
        private Dictionary<string, SearchParameter> BuildSearchParameterDefinitionLookup()
        {
            var parser = new FhirJsonParser();

            string bundleContext = null;
            try
            {
                bundleContext = File.ReadAllText(_searchParameterFile);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Read search parameter file \"{_searchParameterFile}\" failed. Reason: {ex.Message}.");
                throw new FhirSpecificationProviderException($"Read search parameter file \"{_searchParameterFile}\" failed. Reason: {ex.Message}.", ex);
            }

            Bundle bundle;
            try
            {
                bundle = parser.Parse<Bundle>(bundleContext);
            }
            catch (Exception exception)
            {
                _logger.LogError($"Failed to parse parameter bundle from file {_searchParameterFile}.");
                throw new FhirSpecificationProviderException($"Failed to parse parameter bundle from file {_searchParameterFile}.", exception);
            }

            var searchParameterDefinition = new Dictionary<string, SearchParameter>();
            foreach (var searchParameter in bundle.Entry.Select(entryComponent => (SearchParameter)entryComponent.Resource))
            {
                if (searchParameterDefinition.ContainsKey(searchParameter.Url))
                {
                    _logger.LogError(
                        $"There are more than one search parameter definition for {searchParameter.Id} (url: {searchParameter.Url}).");
                    throw new FhirSpecificationProviderException($"There are more than one search parameter definition for {searchParameter.Id} (url: {searchParameter.Url}).");
                }

                searchParameterDefinition[searchParameter.Url] = searchParameter;
            }

            return searchParameterDefinition;
        }

        private string SearchParameterKey(string resourceType, string searchParameter) => $"{resourceType}_{searchParameter}";
    }
}
