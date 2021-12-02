// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.CommonDataModel.ObjectModel.Cdm;
using Microsoft.Health.Fhir.Transformation.Core;
using Microsoft.Health.Fhir.Transformation.Core.TabularDefinition;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Cdm.Test
{
    [TestClass]
    public class CdmTest
    {
        [TestMethod]
        public async Task GivenConfigurationSetAndFhirResources_WhenTransform_CdmFolderShouldBeGenerated()
        {
            string cdmFolder = Guid.NewGuid().ToString("N");
            BaseMappingDefinitionLoader loader = new LocalMappingDefinitionLoader("TestResource\\testSchema");
            IEnumerable<TabularMappingDefinition> tabularMappings = loader.Load();
            CdmCorpusDefinition defination = CdmSchemaGenerator.InitLocalcdmCorpusDefinition(cdmFolder);

            CdmSchemaGenerator cdmSchemaGenerator = new CdmSchemaGenerator(defination);
            await cdmSchemaGenerator.InitializeCdmFolderAsync(tabularMappings);

            ISource source = new LocalNdjsonSource(Path.Combine("TestResource", "FhirResource"));
            ISink sink = new LocalCsvSink(cdmFolder)
            {
                CsvFilePath = (string tableName) =>
                {
                    return $"data/Local{tableName}/{tableName}-partition-data.csv";
                }
            };
            var transformer = new BasicFhirElementTabularTransformer();
            FhirElementTabularTransformer.IdGenerator = () =>
            {
                return "0000";
            };
            TransformationExecutor executor = new TransformationExecutor(source, sink, tabularMappings, transformer);

            await executor.ExecuteAsync();

            // Whether generated CDM schema
            Assert.IsTrue(File.Exists(Path.Combine(cdmFolder, "default.manifest.cdm.json")));
            Assert.IsTrue(File.Exists(Path.Combine(cdmFolder, "LocalPatient.cdm.json")));
            Assert.IsTrue(File.Exists(Path.Combine(cdmFolder, "LocalPatientName.cdm.json")));

            // If generated flatten data
            Assert.IsTrue(Directory.Exists(Path.Combine(cdmFolder, "data", "LocalPatient")));
            Assert.IsTrue(Directory.Exists(Path.Combine(cdmFolder, "data", "LocalPatientName")));

            // If generated data are same with ground truth
            string[] tableNames ={
                "Patient",
                "AllergyIntolerance",
                "CarePlan",
                "Encounter",
                "Location",
                "Observation",

                "PatientName",
                "PatientFlattenJson",
                "EncounterClass",
                "CarePlanPeriod"
            };

            foreach (var tableName in tableNames)
            {
                var sourceFilePath = Path.Combine("TestResource", "TestOutput", $"Local{tableName}", $"{tableName}-partition-data.csv");
                var targetFilePath = Path.Combine(@$"{cdmFolder}", "data", $"Local{tableName}", $"{tableName}-partition-data.csv");
                Assert.IsTrue(CheckSameContent(sourceFilePath, targetFilePath), $"{sourceFilePath} and {targetFilePath} are different.");
            }
        }

        private bool CheckSameContent(string sourceFilePath, string targetFilePath)
        {
            if (!File.Exists(sourceFilePath) || !File.Exists(targetFilePath))
            {
                return false;
            }
            int sourceByte = 0;
            int targetByte = 0;
            using FileStream sourceStream = new FileStream(sourceFilePath, FileMode.Open);
            using FileStream targetStream = new FileStream(targetFilePath, FileMode.Open);
            while ((sourceByte != -1) && (targetByte != -1) && (sourceByte == targetByte))
            {
                sourceByte = sourceStream.ReadByte();
                targetByte = targetStream.ReadByte();
            }
            sourceStream.Close();
            targetStream.Close();
            return sourceByte == targetByte;
        }
    }
}
