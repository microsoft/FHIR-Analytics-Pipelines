// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Health.Fhir.Transformation.Core;

namespace Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor
{
    public class StorageDefinitionLoader : BaseMappingDefinitionLoader
    {
        private Uri _accountUri;
        private string _container;
        private TokenCredential _credential;

        public StorageDefinitionLoader(Uri accountUri, string container, TokenCredential credential, int maxDepth) : base(maxDepth)
        {
            _accountUri = accountUri;
            _container = container;
            _credential = credential;
        }

        public override IEnumerable<string> LoadPropertiesGroupsContent()
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(_accountUri, _credential);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(_container);
            foreach (BlobItem blobItem in containerClient.GetBlobs(prefix: $"{ConfigurationConstants.PropertiesGroupFolderName}/"))
            {
                if (IsConfigFile(blobItem.Name))
                {
                    var blobClient = containerClient.GetBlobClient(blobItem.Name);
                    using Stream content = blobClient.Download().Value.Content;
                    using StreamReader reader = new StreamReader(content);
                    yield return reader.ReadToEnd();
                }
            }
        }

        public override IEnumerable<string> LoadTableDefinitionsContent()
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(_accountUri, _credential);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(_container);
            foreach (BlobItem blobItem in containerClient.GetBlobs())
            {
                if (IsRootFile(blobItem.Name) && IsConfigFile(blobItem.Name))
                {
                    var blobClient = containerClient.GetBlobClient(blobItem.Name);
                    using Stream content = blobClient.Download().Value.Content;
                    using StreamReader reader = new StreamReader(content);
                    yield return reader.ReadToEnd();
                }
            }
        }

        private static bool IsRootFile(string filePath)
        {
            return !filePath.Contains('/');
        }

        private static bool IsConfigFile(string filePath)
        {
            return ".json".Equals(Path.GetExtension(filePath));
        }
    }
}
