// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public static class DimensionNames
    {
        /// <summary>
        /// A metric dimension for a specific metric name.
        /// </summary>
        public static string Name => nameof(DimensionNames.Name);

        /// <summary>
        /// An metric dimension for the detail component.
        /// </summary>
        public static string Component => nameof(DimensionNames.Component);

        /// <summary>
        /// An metric dimension for category
        /// </summary>
        public static string Category => nameof(DimensionNames.Category);

        /// <summary>
        /// A metric dimension for a error type.
        /// </summary>
        public static string ErrorType => nameof(DimensionNames.ErrorType);

        /// <summary>
        /// A metric dimension that represents the reason that caused the metric to be emitted.
        /// </summary>
        public static string Reason => nameof(Reason);

        /// <summary>
        /// A metric dimension that represents the metric if for internal diagnostic.
        /// </summary>
        public static string IsDiagnostic => nameof(DimensionNames.IsDiagnostic);

        /// <summary>
        /// A metric dimension that represents stage of synapse pipeline.
        /// </summary>
        public static string Operation => nameof(DimensionNames.Operation);
    }
}
