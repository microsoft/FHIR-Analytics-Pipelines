// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet
{
    public class FhirParquetSchemaManager : IFhirSchemaManager<FhirParquetSchemaNode>
    {
        private readonly ParquetSchemaProviderDelegate _schemaProviderDelegate;
        private readonly bool _enableCustomizedSchema;
        private readonly ILogger<FhirParquetSchemaManager> _logger;

        private Dictionary<string, List<string>> _schemaTypesMap;
        private Dictionary<string, FhirParquetSchemaNode> _resourceSchemaNodesMap;

        public FhirParquetSchemaManager(
            IOptions<SchemaConfiguration> schemaConfiguration,
            ParquetSchemaProviderDelegate parquetSchemaDelegate,
            ILogger<FhirParquetSchemaManager> logger)
        {
            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));

            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _schemaProviderDelegate = EnsureArg.IsNotNull(parquetSchemaDelegate, nameof(parquetSchemaDelegate));
            _enableCustomizedSchema = schemaConfiguration.Value.EnableCustomizedSchema;
        }

        // Lazy load schemas for retrying to access ACR
        private Dictionary<string, FhirParquetSchemaNode> ResourceSchemaNodesMap
        {
            get
            {
                if (_resourceSchemaNodesMap is null)
                {
                    _resourceSchemaNodesMap = LoadSchemaMap();
                }

                return _resourceSchemaNodesMap;
            }
            set => _resourceSchemaNodesMap = value;
        }

        private Dictionary<string, List<string>> SchemaTypesMap
        {
            get
            {
                if (_schemaTypesMap is null)
                {
                    _schemaTypesMap = LoadSchemaTypeMap(ResourceSchemaNodesMap);
                }

                return _schemaTypesMap;
            }
            set => _schemaTypesMap = value;
        }

        public List<string> GetSchemaTypes(string resourceType)
        {
            if (!SchemaTypesMap.ContainsKey(resourceType))
            {
                _logger.LogError($"Schema types for {resourceType} is empty.");
                return new List<string>();
            }

            return SchemaTypesMap[resourceType];
        }

        public FhirParquetSchemaNode GetSchema(string schemaType)
        {
            if (!ResourceSchemaNodesMap.ContainsKey(schemaType))
            {
                _logger.LogError($"Schema for schema type {schemaType} is not supported.");
                return null;
            }

            return ResourceSchemaNodesMap[schemaType];
        }

        public Dictionary<string, string> GetAllSchemaContent()
        {
            return ResourceSchemaNodesMap.ToDictionary(pair => pair.Key, pair => Newtonsoft.Json.JsonConvert.SerializeObject(pair.Value));
        }

        public Dictionary<string, FhirParquetSchemaNode> GetAllSchemas()
        {
            return new Dictionary<string, FhirParquetSchemaNode>(ResourceSchemaNodesMap);
        }

        private Dictionary<string, FhirParquetSchemaNode> LoadSchemaMap()
        {
            var defaultSchemaProvider = _schemaProviderDelegate(FhirParquetSchemaConstants.DefaultSchemaProviderKey);

            // Get default schema, the default schema keys are resource types, like "Patient", "Encounter".
            var resourceSchemaNodesMap = defaultSchemaProvider.GetSchemasAsync().Result;
            _logger.LogInformation($"{resourceSchemaNodesMap.Count} resource default schemas have been loaded.");

            if (_enableCustomizedSchema)
            {
                var customizedSchemaProvider = _schemaProviderDelegate(FhirParquetSchemaConstants.CustomSchemaProviderKey);

                // Get customized schema, the customized schema keys are resource types with "_customized" suffix, like "Patient_Customized", "Encounter_Customized".
                var resourceCustomizedSchemaNodesMap = customizedSchemaProvider.GetSchemasAsync().Result;

                _logger.LogInformation($"{resourceCustomizedSchemaNodesMap.Count} resource customized schemas have been loaded.");
                resourceSchemaNodesMap = resourceSchemaNodesMap.Concat(resourceCustomizedSchemaNodesMap).ToDictionary(x => x.Key, x => x.Value);
            }

            return resourceSchemaNodesMap;
        }

        private static Dictionary<string, List<string>> LoadSchemaTypeMap(Dictionary<string, FhirParquetSchemaNode> schemaMap)
        {
            var schemaTypesMap = new Dictionary<string, List<string>>();

            // Each resource type may map to multiple output schema types
            // E.g: Schema type list for "Patient" resource can be ["Patient", "Patient_Customized"]
            foreach (var schemaNodeItem in schemaMap)
            {
                if (schemaTypesMap.ContainsKey(schemaNodeItem.Value.Type))
                {
                    schemaTypesMap[schemaNodeItem.Value.Type].Add(schemaNodeItem.Key);
                }
                else
                {
                    schemaTypesMap.Add(schemaNodeItem.Value.Type, new List<string>() { schemaNodeItem.Key });
                }
            }

            return schemaTypesMap;
        }
    }
}
