// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders
{
    public delegate IFhirSpecificationProvider FhirSpecificationProviderDelegate(FhirVersion fhirVersion);

    public interface IFhirSpecificationProvider
    {
        /// <summary>
        /// Get all the resource types
        /// </summary>
        /// <returns>the resource types.</returns>
        public IEnumerable<string> GetAllResourceTypes();

        /// <summary>
        /// check if the resource type is a valid FHIR resource type.
        /// </summary>
        /// <param name="resourceType">the resource type</param>
        /// <returns>return true if it is a valid FHIR resource type.</returns>
        public bool IsValidFhirResourceType(string resourceType);

        /// <summary>
        /// Get all the resource types pertaining to the specified compartmentType,
        /// which is fetched from FHIR definitions. http://hl7.org/fhir/R4/compartmentdefinition.html
        /// </summary>
        /// <param name="compartmentType">the compartment type.</param>
        /// <returns>the resource types.</returns>
        public IEnumerable<string> GetCompartmentResourceTypes(string compartmentType);

        /// <summary>
        /// Get the supported search parameters of the specified resource type,
        /// which is fetched from FHIR server metadata.
        /// </summary>
        /// <param name="resourceType">the resource type.</param>
        /// <returns>the search parameters</returns>
        public IEnumerable<string> GetSearchParametersByResourceType(string resourceType);
    }
}
