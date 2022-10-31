// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirR4;

using System;
using System.Collections.Generic;
using System.Linq;
using FhirR4::Hl7.Fhir.Model;
using FhirR4::Hl7.Fhir.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient;
using R4FhirModelInfo = FhirR4::Hl7.Fhir.Model.ModelInfo;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders
{
    // TODO: Maybe we can get all FHIR specification directly through 'metadata' API. Thus we can remove the hard dependency with FHIR libs (which targets specific version)
    public class R4FhirSpecificationProvider : BaseFhirSpecificationProvider
    {
        /// <summary>
        /// Download from http://hl7.org/fhir/R4/compartmentdefinition-patient.json
        /// </summary>
        protected override IEnumerable<string> _compartmentEmbeddedFiles { get; } = new List<string> { "Specifications.R4.compartmentdefinition-patient.json" };

        /// <summary>
        /// Download from http://hl7.org/fhir/R4/search-parameters.json, which is defined in http://hl7.org/fhir/R4/searchparameter-registry.html
        /// </summary>
        protected override string _searchParameterEmbeddedFile { get; } = "Specifications.R4.search-parameters.json";

        public R4FhirSpecificationProvider(IFhirDataClient dataClient, IDiagnosticLogger diagnosticLogger, ILogger<R4FhirSpecificationProvider> logger)
            : base(dataClient, diagnosticLogger, logger)
        {
        }

        public override IEnumerable<string> GetAllResourceTypes()
        {
            return R4FhirModelInfo.SupportedResources.Except(ExcludeTypes);
        }

        public override bool IsValidFhirResourceType(string resourceType)
        {
            return R4FhirModelInfo.IsKnownResource(resourceType);
        }

        protected override bool IsValidCompartmentType(string compartmentType)
        {
            return compartmentType != null && Enum.IsDefined(typeof(CompartmentType), compartmentType);
        }

        protected override Dictionary<string, HashSet<string>> BuildCompartmentResourceTypesLookupFromCompartmentContext(string compartmentContext, string compartmentFile)
        {
            FhirJsonParser parser = new FhirJsonParser();
            Dictionary<string, HashSet<string>> compartmentResourceTypesLookup = new Dictionary<string, HashSet<string>>();

            CompartmentDefinition compartment;

            try
            {
                compartment = parser.Parse<CompartmentDefinition>(compartmentContext);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, $"Failed to parse compartment definition from file {compartmentFile}. Reason: {exception.Message}");
                throw new FhirSpecificationProviderException($"Failed to parse compartment definition from file {compartmentFile}.", exception);
            }

            string compartmentType = compartment.Code?.ToString();
            if (IsValidCompartmentType(compartmentType))
            {
                HashSet<string> resourceTypes = compartment.Resource?.Where(x => x.Param.Any()).Select(x => x.Code?.ToString()).ToHashSet();
                if (resourceTypes == null)
                {
                    _logger.LogInformation($"There is not any resource type defined for compartment type {compartmentType} in file {compartmentFile}");
                }
                else
                {
                    compartmentResourceTypesLookup.Add(compartmentType, resourceTypes);
                    _logger.LogInformation($"There are {resourceTypes.Count} resources type pertained to compartment type {compartmentType}.");
                }
            }
            else
            {
                _logger.LogInformation($"The compartment type {compartmentType} in file {compartmentFile} isn't a valid compartment type.");
            }

            return compartmentResourceTypesLookup;
        }

        protected override FhirCapabilityData BuildCapabilityDataFromMetadata(string metaData)
        {
            FhirJsonParser parser = new FhirJsonParser();

            CapabilityStatement capabilityStatement;
            try
            {
                capabilityStatement = parser.Parse<CapabilityStatement>(metaData);
            }
            catch (Exception exception)
            {
                _diagnosticLogger.LogError($"Failed to parse capability statement from FHIR server metadata. Reason: {exception.Message}");
                _logger.LogInformation(exception, $"Failed to parse capability statement from FHIR server metadata. Reason: {exception.Message}");
                throw new FhirSpecificationProviderException($"Failed to parse capability statement from FHIR server metadata.", exception);
            }

            Dictionary<string, HashSet<string>> searchParameters = new Dictionary<string, HashSet<string>>();
            Dictionary<string, string> searchParameterIds = new Dictionary<string, string>();

            List<CapabilityStatement.RestComponent> rest = capabilityStatement?.Rest;
            if (rest == null || !rest.Any())
            {
                _diagnosticLogger.LogError($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
                _logger.LogInformation($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
                throw new FhirSpecificationProviderException($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
            }

            List<CapabilityStatement.ResourceComponent> resources = rest.First().Resource;

            if (resources == null || !resources.Any())
            {
                _diagnosticLogger.LogError($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
                _logger.LogInformation($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
                throw new FhirSpecificationProviderException($"Failed to build SearchParametersLookup: the resource in capabilityStatement is null.");
            }

            foreach (CapabilityStatement.ResourceComponent resource in resources)
            {
                string type = resource.Type?.ToString();
                if (!string.IsNullOrEmpty(type))
                {
                    searchParameters[type] = resource.SearchParam.Select(x => x.Name).ToHashSet();
                    resource.SearchParam.ForEach(x => searchParameterIds[SearchParameterKey(type, x.Name)] = x.Definition);
                }
            }

            if (!searchParameters.Any())
            {
                _diagnosticLogger.LogError("There is not any items in the built SearchParametersLookup.");
                _logger.LogInformation("There is not any items in the built SearchParametersLookup.");
                throw new FhirSpecificationProviderException("There is not any items in the built SearchParametersLookup.");
            }

            _logger.LogInformation($"Build SearchParametersLookup from fhir server metadata successfully.");

            return new FhirCapabilityData(searchParameters, searchParameterIds);
        }

        /// <summary>
        /// build SearchParameterDefinitionLookup, search parameter url to searchParameter object
        /// </summary>
        /// <returns>search parameter url to searchParameter object dictionary</returns>
        private Dictionary<string, SearchParameter> BuildSearchParameterDefinitionLookup()
        {
            string bundleContext;
            try
            {
                bundleContext = LoadEmbeddedSpecification(_searchParameterEmbeddedFile);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Read search parameter file \"{_searchParameterEmbeddedFile}\" failed. Reason: {ex.Message}.");
                throw new FhirSpecificationProviderException($"Read search parameter file \"{_searchParameterEmbeddedFile}\" failed. Reason: {ex.Message}.", ex);
            }

            FhirJsonParser parser = new FhirJsonParser();
            Bundle bundle;
            try
            {
                bundle = parser.Parse<Bundle>(bundleContext);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, $"Failed to parse parameter bundle from file {_searchParameterEmbeddedFile}. Reason: {exception.Message}.");
                throw new FhirSpecificationProviderException($"Failed to parse parameter bundle from file {_searchParameterEmbeddedFile}.", exception);
            }

            Dictionary<string, SearchParameter> searchParameterDefinition = new Dictionary<string, SearchParameter>();
            if (bundle.Entry == null)
            {
                _logger.LogError($"Failed to build SearchParameterDefinitionLookup from file {_searchParameterEmbeddedFile}, the bundle entry is null.");
                throw new FhirSpecificationProviderException($"Failed to build SearchParameterDefinitionLookup from file {_searchParameterEmbeddedFile}, the bundle entry is null.");
            }

            foreach (SearchParameter searchParameter in bundle.Entry.Select(entryComponent => (SearchParameter)entryComponent.Resource))
            {
                if (searchParameter == null)
                {
                    _logger.LogInformation("The search parameter is null, just ignore it.");
                    continue;
                }

                if (searchParameter.Url == null)
                {
                    _logger.LogInformation($"The search parameter URL is null in {searchParameter}, just ignore it.");
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
    }
}
