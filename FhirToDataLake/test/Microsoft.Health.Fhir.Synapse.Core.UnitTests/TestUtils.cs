// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests
{
    public static class TestUtils
    {
        public const string DefaultSchemaDirectoryPath = "../../../../../data/schemas";
        public const string TestSchemaDirectoryPath = "./TestData/schemas";

        public static IEnumerable<JObject> LoadNdjsonData(string filePath)
        {
            // Date formatted strings are not parsed to a date type and are read as strings,
            // to prevent output from being affected by time zone.
            var serializerSettings = new JsonSerializerSettings { DateParseHandling = DateParseHandling.None };

            foreach (var line in File.ReadAllLines(filePath))
            {
                yield return JsonConvert.DeserializeObject<JObject>(line, serializerSettings);
            }
        }

        public static IParquetSchemaProvider MockParquetSchemaProviderDelegate(string name)
        {
            return new LocalDefaultSchemaProvider(NullLogger<LocalDefaultSchemaProvider>.Instance);
        }
    }
}
