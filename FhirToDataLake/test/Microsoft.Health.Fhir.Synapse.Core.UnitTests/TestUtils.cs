// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests
{
    public static class TestUtils
    {
        public const string SchemaDirectoryPath = "../../../../../data/schemas";

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

        public static TaskContext GetTestTaskContext(string resourceType)
        {
            return new TaskContext(
                null,
                null,
                resourceType,
                new DateTimeOffset(DateTime.MinValue, TimeSpan.Zero),
                new DateTimeOffset(DateTime.MinValue, TimeSpan.Zero),
                null,
                0,
                0,
                0);
        }
    }
}
