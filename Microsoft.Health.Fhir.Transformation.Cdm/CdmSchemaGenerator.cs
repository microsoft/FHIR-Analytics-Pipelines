// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.CommonDataModel.ObjectModel.Cdm;
using Microsoft.CommonDataModel.ObjectModel.Enums;
using Microsoft.CommonDataModel.ObjectModel.Storage;
using Microsoft.Health.Fhir.Transformation.Core;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Transformation.Cdm
{
    public class CdmSchemaGenerator
    {
        private const string FoundationJsonPath = "cdm:/foundations.cdm.json";

        private CdmCorpusDefinition _cdmCorpusDefinition;

        public CdmSchemaGenerator(CdmCorpusDefinition cdmCorpusDefinition)
        {
            _cdmCorpusDefinition = cdmCorpusDefinition;
        }

        public async Task<List<string>> InitializeCdmFolderAsync(IEnumerable<TabularMappingDefinition> tabularMappings, string rootFolder = "local")
        {
            _cdmCorpusDefinition.SetEventCallback(null, CdmStatusLevel.None);
            CdmManifestDefinition manifestAbstract = _cdmCorpusDefinition.MakeObject<CdmManifestDefinition>(CdmObjectType.ManifestDef, "tempAbstract");

            var localRoot = _cdmCorpusDefinition.Storage.FetchRootFolder(rootFolder);
            localRoot.Documents.Add(manifestAbstract);

            List<string> entities = await BuildCdmEntitys(_cdmCorpusDefinition, manifestAbstract, localRoot, tabularMappings);
            CdmManifestDefinition manifestResolved = await CreateResolvedManifest(manifestAbstract);
            await CreateDataPatitions(_cdmCorpusDefinition, manifestResolved);
            await manifestResolved.SaveAsAsync($"{manifestResolved.ManifestName}.manifest.cdm.json", true);

            return entities;
        }

        public static CdmCorpusDefinition InitLocalcdmCorpusDefinition(string cdmRoot)
        {
            var cdmCorpus = new CdmCorpusDefinition();
            cdmCorpus.Storage.Mount("local", new LocalAdapter(cdmRoot));
            cdmCorpus.Storage.Mount("cdm", new CdmStandardsAdapter());
            cdmCorpus.Storage.DefaultNamespace = "local";
            return cdmCorpus;
        }

        private static async Task<CdmManifestDefinition> CreateResolvedManifest(CdmManifestDefinition manifestAbstract)
        {
            var manifestResolved = await manifestAbstract.CreateResolvedManifestAsync("default", null);
            manifestResolved.Imports.Add(FoundationJsonPath);
            return manifestResolved;
        }

        private static async Task CreateDataPatitions(CdmCorpusDefinition cdmCorpus, CdmManifestDefinition manifestResolved)
        {
            foreach (CdmEntityDeclarationDefinition eDef in manifestResolved.Entities)
            {
                var localEDef = eDef;
                var entDef = await cdmCorpus.FetchObjectAsync<CdmEntityDefinition>(localEDef.EntityPath, manifestResolved);
                var part = cdmCorpus.MakeObject<CdmDataPartitionDefinition>(CdmObjectType.DataPartitionDef, $"{entDef.EntityName}-data-description");
                localEDef.DataPartitions.Add(part);
                part.Location = $"data/{entDef.EntityName}/partition-data-*.csv";
                var csvTrait = part.ExhibitsTraits.Add("is.partition.format.CSV", false);
                csvTrait.Arguments.Add("columnHeaders", "true");
                csvTrait.Arguments.Add("delimiter", ",");
            }
        }

        private static async Task<List<string>> BuildCdmEntitys(CdmCorpusDefinition cdmCorpus, CdmManifestDefinition manifestAbstract, CdmFolderDefinition localRoot, IEnumerable<TabularMappingDefinition> tabularMappings)
        {
            List<string> results = new List<string>();
            foreach (TabularMappingDefinition tabularMapping in tabularMappings)
            {
                var entity = cdmCorpus.MakeObject<CdmEntityDefinition>(CdmObjectType.EntityDef, tabularMapping.TableName, false);

                foreach ((string columnName, string type) in tabularMapping.Columns)
                {
                    var attribute = CreateEntityAttributeWithPurposeAndDataType(cdmCorpus, columnName, string.Empty, CdmDataTypeHelper.ConvertFhirTypeToCdmType(type));
                    entity.Attributes.Add(attribute);
                }

                var entityDoc = cdmCorpus.MakeObject<CdmDocumentDefinition>(CdmObjectType.DocumentDef, $"{tabularMapping.TableName}.cdm.json", false);
                entityDoc.Imports.Add(FoundationJsonPath);
                entityDoc.Definitions.Add(entity);
                localRoot.Documents.Add(entityDoc, entityDoc.Name);

                string entityName = $"Local{tabularMapping.TableName}";
                var resolvedEntity = await entity.CreateResolvedEntityAsync(entityName);
                manifestAbstract.Entities.Add(resolvedEntity);
                results.Add(entityName);
            }

            return results;
        }

        private static CdmTypeAttributeDefinition CreateEntityAttributeWithPurposeAndDataType(CdmCorpusDefinition cdmCorpus, string attributeName, string purpose, string dataType)
        {
            var entityAttribute = CreateEntityAttributeWithPurpose(cdmCorpus, attributeName, purpose);
            entityAttribute.DataType = cdmCorpus.MakeRef<CdmDataTypeReference>(CdmObjectType.DataTypeRef, dataType, true);
            return entityAttribute;
        }

        private static CdmTypeAttributeDefinition CreateEntityAttributeWithPurpose(CdmCorpusDefinition cdmCorpus, string attributeName, string purpose)
        {
            var entityAttribute = cdmCorpus.MakeObject<CdmTypeAttributeDefinition>(CdmObjectType.TypeAttributeDef, attributeName, false);
            entityAttribute.Purpose = cdmCorpus.MakeRef<CdmPurposeReference>(CdmObjectType.PurposeRef, purpose, true);
            return entityAttribute;
        }
    }
}
