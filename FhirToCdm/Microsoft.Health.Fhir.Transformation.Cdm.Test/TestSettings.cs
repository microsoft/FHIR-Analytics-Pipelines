// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Runtime.Serialization;

namespace Microsoft.Health.Fhir.Transformation.Cdm.Test
{
    [DataContract]
    public class TestSettings
    {
        [DataMember(Name ="clientId")]
        public string ClientId
        {
            get;
            set;
        }

        [DataMember(Name = "tenantId")]
        public string TenantId
        {
            get;
            set;
        }

        [DataMember(Name = "secret")]
        public string Secret
        {
            get;
            set;
        }

        [DataMember(Name = "adlsAccount")]
        public string AdlsAccountName
        {
            get;
            set;
        }

        [DataMember(Name = "blobUri")]
        public string BlobUri
        {
            get;
            set;
        }
    }
}
