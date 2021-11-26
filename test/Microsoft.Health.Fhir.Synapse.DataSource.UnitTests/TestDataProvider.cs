// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.IO;
using Hl7.Fhir.ElementModel;
using Microsoft.Health.Fhir.Synapse.DataSerialization;

namespace Microsoft.Health.Fhir.Synapse.DataSource.UnitTests
{
    public static class TestDataProvider
    {
        public static string GetBundleFromFile(string filePath)
        {
            return File.ReadAllText(filePath);
        }

        public static ITypedElement GetBundleElementFromFile(string filePath)
        {
            var bundleContent = GetBundleFromFile(filePath);
            var fhirSerializer = new FhirSerializer();

            return fhirSerializer.DeserializeToElement(bundleContent);
        }
    }
}
