// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.IO;
using System.Text;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Extensions
{
    public static class JobExtensions
    {
        public static Stream ToStream(this Job job)
        {
            var content = JsonConvert.SerializeObject(job);
            return new MemoryStream(Encoding.UTF8.GetBytes(content));
        }
    }
}
