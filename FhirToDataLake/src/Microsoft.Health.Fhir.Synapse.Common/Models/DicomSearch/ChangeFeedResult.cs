// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.DicomSearch
{
    public class ChangeFeedResult
    {
        /// <summary>
        /// The result metadata content.
        /// </summary>
        public string Metadata { get; set; }

        /// <summary>
        /// The timestamp of result action.
        /// </summary>
        public DateTimeOffset Timestamp { get; set; }

        /// <summary>
        /// The sequence of result change feed.
        /// </summary>
        public long Sequence { get; set; }
    }
}
