// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow
{
    /// <summary>
    /// The handling options when unexpected fields are encountered in input data.
    /// </summary>
    public enum UnexpectedFieldBehaviorOptions
    {
        /// <summary>
        /// Unexpected JSON fields are ignored.
        /// </summary>
        Ignore,

        /// <summary>
        /// Unexpected JSON fields error out. Currently this option is not supported.
        /// </summary>
        Error,

        /// <summary>
        /// Unexpected JSON fields are type-inferred and included in the output. Currently this option is not supported.
        /// </summary>
        InferType,
    }

    public class ArrowParseOptionsConfiguration
    {
        [JsonProperty("unexpectedFieldBehavior")]
        public UnexpectedFieldBehaviorOptions UnexpectedFieldBehavior { get; set; } = UnexpectedFieldBehaviorOptions.Ignore;
    }
}
