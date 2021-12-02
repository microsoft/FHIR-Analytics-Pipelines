// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;

namespace Microsoft.Health.Fhir.Transformation.Core.TabularDefinition
{
    public class LocalMappingDefinitionLoader : BaseMappingDefinitionLoader
    {
        private string _folderPath;

        public LocalMappingDefinitionLoader(string folderPath, int maxDepth = 3) : base(maxDepth)
        {
            _folderPath = folderPath;
        }

        public override IEnumerable<string> LoadPropertiesGroupsContent()
        {
            string propertiesGroupFolder = Path.Combine(_folderPath, ConfigurationConstants.PropertiesGroupFolderName);
            foreach (var fileName in Directory.EnumerateFiles(propertiesGroupFolder, "*.json"))
            {
                yield return File.ReadAllText(fileName);
            }
        }

        public override IEnumerable<string> LoadTableDefinitionsContent()
        {
            foreach (var fileName in Directory.EnumerateFiles(_folderPath, "*.json"))
            {
                yield return File.ReadAllText(fileName);
            }
        }
    }
}
