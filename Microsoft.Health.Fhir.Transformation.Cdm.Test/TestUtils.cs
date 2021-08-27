// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.IO;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Transformation.Cdm.Test
{
    public static class TestUtils
    {
        public static TestSettings LoadTestSettings(string settingsFile = "test-settings.json")
        {
            return JsonConvert.DeserializeObject<TestSettings>(File.ReadAllText(settingsFile));
        }
    }
}
