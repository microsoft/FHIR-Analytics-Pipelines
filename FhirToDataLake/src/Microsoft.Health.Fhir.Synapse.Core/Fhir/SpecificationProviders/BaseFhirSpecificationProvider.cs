// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders
{
    public abstract class BaseFhirSpecificationProvider : IFhirSpecificationProvider
    {
        private readonly IApiDataClient _dataClient;

        /// <summary>
        /// The resource types of each compartment type, extracted from _compartmentFiles
        /// </summary>
        private readonly Dictionary<string, HashSet<string>> _compartmentResourceTypesLookup;

        /// <summary>
        /// Capability data.
        /// </summary>
        private readonly Lazy<FhirCapabilityData> _capabilityData;

        protected static readonly IEnumerable<string> ExcludeTypes = new List<string>
        {
            FhirConstants.StructureDefinitionResource,
            FhirConstants.OperationOutcomeResource,
        };

        public BaseFhirSpecificationProvider(
            IApiDataClient dataClient,
            IDiagnosticLogger diagnosticLogger,
            ILogger logger)
        {
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            DiagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            Logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _compartmentResourceTypesLookup = BuildCompartmentResourceTypesLookup();
            _capabilityData = new Lazy<FhirCapabilityData>(() => BuildFhirCapabilityData());
        }

        protected IDiagnosticLogger DiagnosticLogger { get; }

        protected ILogger Logger { get; }

        protected abstract IEnumerable<string> CompartmentEmbeddedFiles { get; }

        protected abstract string SearchParameterEmbeddedFile { get; }

        public abstract IEnumerable<string> GetAllResourceTypes();

        public abstract bool IsValidFhirResourceType(string resourceType);

        public virtual IEnumerable<string> GetCompartmentResourceTypes(string compartmentType)
        {
            if (!IsValidCompartmentType(compartmentType))
            {
                DiagnosticLogger.LogError($"The compartment type {compartmentType} isn't a valid compartment type.");
                Logger.LogInformation($"The compartment type {compartmentType} isn't a valid compartment type.");
                throw new FhirSpecificationProviderException($"The compartment type {compartmentType} isn't a valid compartment type.");
            }

            if (!_compartmentResourceTypesLookup.ContainsKey(compartmentType))
            {
                DiagnosticLogger.LogError($"The compartment type {compartmentType} isn't supported now.");
                Logger.LogInformation($"The compartment type {compartmentType} isn't supported now.");
                throw new FhirSpecificationProviderException($"The compartment type {compartmentType} isn't supported now.");
            }

            return _compartmentResourceTypesLookup[compartmentType];
        }

        public virtual IEnumerable<string> GetSearchParametersByResourceType(string resourceType)
        {
            if (!IsValidFhirResourceType(resourceType))
            {
                DiagnosticLogger.LogError($"The input {resourceType} isn't a valid resource type.");
                Logger.LogInformation($"The input {resourceType} isn't a valid resource type.");
                throw new FhirSpecificationProviderException($"The input {resourceType} isn't a valid resource type.");
            }

            if (!_capabilityData.Value.ResourceTypeSearchParametersLookup.ContainsKey(resourceType))
            {
                Logger.LogInformation($"There is no search parameter defined for resource type {resourceType}.");
                return new HashSet<string>();
            }

            return _capabilityData.Value.ResourceTypeSearchParametersLookup[resourceType];
        }

        protected virtual Dictionary<string, HashSet<string>> BuildCompartmentResourceTypesLookup()
        {
            Dictionary<string, HashSet<string>> compartmentResourceTypesLookup = new Dictionary<string, HashSet<string>>();

            foreach (string compartmentFile in CompartmentEmbeddedFiles)
            {
                string compartmentContext;
                try
                {
                    compartmentContext = LoadEmbeddedSpecification(compartmentFile);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, $"Read compartment file \"{compartmentFile}\" failed. Reason: {ex.Message}.");
                    throw new FhirSpecificationProviderException($"Read compartment file \"{compartmentFile}\" failed. Reason: {ex.Message}.", ex);
                }

                Dictionary<string, HashSet<string>> subCompartmentResourceTypesLookup = BuildCompartmentResourceTypesLookupFromCompartmentContext(compartmentContext, compartmentFile);
                foreach (KeyValuePair<string, HashSet<string>> item in subCompartmentResourceTypesLookup)
                {
                    compartmentResourceTypesLookup.Add(item.Key, item.Value);
                }
            }

            return compartmentResourceTypesLookup;
        }

        /// <summary>
        /// Retrieve Fhir server metadata and build _resourceTypeSearchParametersLookup based on it.
        /// </summary>
        /// <returns>The capability data look up result.</returns>
        protected virtual FhirCapabilityData BuildFhirCapabilityData()
        {
            var metadataOptions = new MetadataOptions();

            string metaData;
            try
            {
                metaData = _dataClient.Search(metadataOptions);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception, $"Failed to request Fhir server metadata. Reason: {exception.Message}.");
                throw new FhirSpecificationProviderException($"Failed to request Fhir server metadata.", exception);
            }

            return BuildCapabilityDataFromMetadata(metaData);
        }

        protected virtual string SearchParameterKey(string resourceType, string searchParameter) => $"{resourceType}_{searchParameter}";

        protected string LoadEmbeddedSpecification(string specificationName)
        {
            // Dictionary<string, string> embeddedSchema = new Dictionary<string, string>();
            var executingAssembly = Assembly.GetExecutingAssembly();
            string specificationKey = string.Format("{0}.{1}", executingAssembly.GetName().Name, specificationName);
            using (Stream stream = executingAssembly.GetManifestResourceStream(specificationKey))
            using (var reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }

        protected abstract FhirCapabilityData BuildCapabilityDataFromMetadata(string metaData);

        protected abstract Dictionary<string, HashSet<string>> BuildCompartmentResourceTypesLookupFromCompartmentContext(string compartmentContext, string compartmentFile);

        protected abstract bool IsValidCompartmentType(string compartmentType);
    }
}
