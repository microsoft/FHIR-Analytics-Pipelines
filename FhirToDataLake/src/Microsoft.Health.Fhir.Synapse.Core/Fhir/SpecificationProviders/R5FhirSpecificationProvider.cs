// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirR5;

using System;
using System.Collections.Generic;
using System.Linq;
using FhirR5::Hl7.Fhir.Model;
using FhirR5::Hl7.Fhir.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient;
using R5FhirModelInfo = FhirR5::Hl7.Fhir.Model.ModelInfo;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders
{
    public class R5FhirSpecificationProvider : BaseFhirSpecificationProvider
    {
        /// <summary>
        /// Download from https://hl7.org/fhir/5.0.0-snapshot1/compartmentdefinition-patient.json.html
        /// </summary>
        protected override IEnumerable<string> _compartmentEmbeddedFiles { get; } = new List<string> { "Specifications.R5.compartmentdefinition-patient.json" };

        /// <summary>
        /// Download from https://hl7.org/fhir/5.0.0-snapshot1/search-parameters.json, which is defined in https://hl7.org/fhir/5.0.0-snapshot1/searchparameter.html
        /// </summary>
        protected override string _searchParameterEmbeddedFile { get; } = "Specifications.R5.search-parameters.json";


        public R5FhirSpecificationProvider(IFhirDataClient dataClient, IDiagnosticLogger diagnosticLogger, ILogger<R5FhirSpecificationProvider> logger)
            : base(dataClient, diagnosticLogger, logger)
        {
        }

        public override IEnumerable<string> GetAllResourceTypes()
        {
            return R5FhirModelInfo.SupportedResources.Except(ExcludeTypes);
        }

        public override bool IsValidFhirResourceType(string resourceType)
        {
            return R5FhirModelInfo.IsKnownResource(resourceType);
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
    }
}
