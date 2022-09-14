// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirR4;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
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
    // TODO: Maybe we can get all FHIR specification directly through 'metadata' API. Thus we can remove the hard dependency with FHIR libs (which targets specific version)
    public class R4FhirSpecificationProvider : IFhirSpecificationProvider
    {
        private readonly IFhirDataClient _dataClient;
        private readonly ILogger<R4FhirSpecificationProvider> _logger;

        private readonly IEnumerable<string> _excludeTypes = new List<string> { FhirConstants.StructureDefinition };

        /// <summary>
        /// Download from http://hl7.org/fhir/R4/compartmentdefinition-patient.json
        /// </summary>
        private readonly IEnumerable<string> _compartmentEmbeddedFiles = new List<string> { "Specifications.R4.compartmentdefinition-patient.json" };

        /// <summary>
        /// Download from http://hl7.org/fhir/R4/search-parameters.json, which is defined in http://hl7.org/fhir/R4/searchparameter-registry.html
        /// </summary>
        private readonly string _searchParameterEmbeddedFile = "Specifications.R4.search-parameters.json";

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

        /// <summary>
        /// search parameter id to search parameter definition, extracted from _searchParameterFile
        /// </summary>
        // TODO: it is not used now. enable it if we would like do more search parameter validation in pipeline
        private readonly Dictionary<string, SearchParameter> _searchParameterDefinitionLookup;

        public R4FhirSpecificationProvider(
            IFhirDataClient dataClient,
            ILogger<R4FhirSpecificationProvider> logger)
        {
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _compartmentResourceTypesLookup = BuildCompartmentResourceTypesLookup();

            // _searchParameterDefinitionLookup = BuildSearchParameterDefinitionLookup();
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

            foreach (var compartmentFile in _compartmentEmbeddedFiles)
            {
                string compartmentContext;
                try
                {
                    compartmentContext = LoadEmbeddedSpecification(compartmentFile);
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

        /// <summary>
        /// build SearchParameterDefinitionLookup, search parameter url to searchParameter object
        /// </summary>
        /// <returns>search parameter url to searchParameter object dictionary</returns>
        private Dictionary<string, SearchParameter> BuildSearchParameterDefinitionLookup()
        {
            var parser = new FhirJsonParser();

            string bundleContext;
            try
            {
                bundleContext = LoadEmbeddedSpecification(_searchParameterEmbeddedFile);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Read search parameter file \"{_searchParameterEmbeddedFile}\" failed. Reason: {ex.Message}.");
                throw new FhirSpecificationProviderException($"Read search parameter file \"{_searchParameterEmbeddedFile}\" failed. Reason: {ex.Message}.", ex);
            }

            Bundle bundle;
            try
            {
                bundle = parser.Parse<Bundle>(bundleContext);
            }
            catch (Exception exception)
            {
                _logger.LogError($"Failed to parse parameter bundle from file {_searchParameterEmbeddedFile}. Reason: {exception.Message}.");
                throw new FhirSpecificationProviderException($"Failed to parse parameter bundle from file {_searchParameterEmbeddedFile}.", exception);
            }

            var searchParameterDefinition = new Dictionary<string, SearchParameter>();
            if (bundle.Entry == null)
            {
                _logger.LogError($"Failed to build SearchParameterDefinitionLookup from file {_searchParameterEmbeddedFile}, the bundle entry is null.");
                throw new FhirSpecificationProviderException($"Failed to build SearchParameterDefinitionLookup from file {_searchParameterEmbeddedFile}, the bundle entry is null.");
            }

            foreach (var searchParameter in bundle.Entry.Select(entryComponent => (SearchParameter)entryComponent.Resource))
            {
                if (searchParameter == null)
                {
                    _logger.LogWarning("The search parameter is null, just ignore it.");
                    continue;
                }

                if (searchParameter.Url == null)
                {
                    _logger.LogWarning($"The search parameter URL is null in {searchParameter}, just ignore it.");
                    continue;
                }

                if (searchParameterDefinition.ContainsKey(searchParameter.Url))
                {
                    _logger.LogError(
                        $"Failed to build SearchParameterDefinitionLookup from file {_searchParameterEmbeddedFile}, there are more than one search parameter definition for {searchParameter.Id} (url: {searchParameter.Url}).");
                    throw new FhirSpecificationProviderException($"Failed to build SearchParameterDefinitionLookup from file {_searchParameterEmbeddedFile}, there are more than one search parameter definition for {searchParameter.Id} (url: {searchParameter.Url}).");
                }

                searchParameterDefinition[searchParameter.Url] = searchParameter;
            }

            _logger.LogInformation($"Build SearchParameterDefinitionLookup successfully.");

            return searchParameterDefinition;
        }

        private string SearchParameterKey(string resourceType, string searchParameter) => $"{resourceType}_{searchParameter}";

        private string LoadEmbeddedSpecification(string specificationName)
        {
            // Dictionary<string, string> embeddedSchema = new Dictionary<string, string>();
            var executingAssembly = Assembly.GetExecutingAssembly();
            string specificationKey = string.Format("{0}.{1}", executingAssembly.GetName().Name, specificationName);
            using (Stream stream = executingAssembly.GetManifestResourceStream(specificationKey))
            using (StreamReader reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }
    }
}
