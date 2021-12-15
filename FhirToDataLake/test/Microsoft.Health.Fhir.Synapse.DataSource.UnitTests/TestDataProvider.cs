// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.IO;
using Hl7.Fhir.ElementModel;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataSource.Fhir;

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

            var fhirConfiguration = new FhirServerConfiguration()
            {
                Version = FhirVersion.R4,
            };
            var fhirSerializer = new FhirSerializer(Options.Create(fhirConfiguration));

            return fhirSerializer.DeserializeToElement(bundleContent);
        }
    }
}
