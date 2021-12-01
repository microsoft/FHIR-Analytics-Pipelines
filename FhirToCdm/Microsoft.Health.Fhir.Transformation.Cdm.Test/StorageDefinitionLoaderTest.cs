// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Health.Fhir.Transformation.BatchExecutor;
using Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Cdm.Test
{
    [TestClass]
    public class StorageDefinitionLoaderTest
    {
        private TestSettings _testSettings;

        public StorageDefinitionLoaderTest()
        {
            _testSettings = TestUtils.LoadTestSettings();
        }

        [TestMethod]
        public async Task GivenAStorageContainerWithConfig_WhenLoadDefinition_ConfigurationShouldBeReturned()
        {
            if (string.IsNullOrEmpty(_testSettings.AdlsAccountName))
            {
                return;
            }

            var credential = Program.GetClientSecretCredential(_testSettings.TenantId, _testSettings.ClientId, _testSettings.Secret);
            string containerName = Guid.NewGuid().ToString("N");
            (int tableCount, int propertiesGroupCount) = await PrepareConfigContainerAsync(credential, containerName);

            string storageServiceUri = $"https://{_testSettings.AdlsAccountName}.blob.core.windows.net";
            var loader = new StorageDefinitionLoader(new Uri(storageServiceUri), containerName, credential, 3);
            var tables = loader.LoadTableDefinitionsContent();
            Assert.IsTrue(tables.First().Length > 0);
            Assert.AreEqual(tableCount, tables.Count());

            var propertiesGroups = loader.LoadPropertiesGroupsContent();
            Assert.IsTrue(propertiesGroups.First().Length > 0);
            Assert.AreEqual(propertiesGroupCount, propertiesGroups.Count());
        }

        private async Task<(int tableCount, int propertiesGroupCount)> PrepareConfigContainerAsync(Azure.Identity.ClientSecretCredential credential, string containerName)
        {
            string storageServiceUri = $"https://{_testSettings.AdlsAccountName}.blob.core.windows.net";
            BlobServiceClient blobServiceClient = new BlobServiceClient(new Uri(storageServiceUri), credential);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.CreateAsync();

            var tableDefinitions = Directory.EnumerateFiles(@"TestResource\TestSchema");
            foreach (string filePath in tableDefinitions)
            {
                await UploadFileAsync(containerClient, filePath);
            }

            var propertiesGroups = Directory.EnumerateFiles(@"TestResource\TestSchema\PropertiesGroup");
            foreach (string filePath in propertiesGroups)
            {
                await UploadFileAsync(containerClient, filePath, "PropertiesGroup/");
            }

            return (tableDefinitions.Count(), propertiesGroups.Count());
        }

        private static async Task UploadFileAsync(BlobContainerClient containerClient, string filePath, string prefix = "")
        {
            string fileName = Path.GetFileName(filePath);
            var blobClient = containerClient.GetBlobClient(prefix + fileName);
            byte[] byteArray = Encoding.ASCII.GetBytes(File.ReadAllText(filePath));
            using MemoryStream stream = new MemoryStream(byteArray);

            await blobClient.UploadAsync(stream);
        }
    }
}
