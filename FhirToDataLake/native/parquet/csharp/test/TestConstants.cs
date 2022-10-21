// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Parquet.UnitTests
{
    public class TestConstants
    {
        public const string InputPatientSmallFile = "./TestData/Patient_small.ndjson";

        public const string InputPatientNormalFile = "./TestData/Patient_normal.ndjson";

        public const string SchemaFile = "./TestData/patient_example_schema.json";

        public const string ExpectedPatientSmallParquetFile = "./TestData/Expected/expected_patient_small.parquet";

        public const string ExpectedPatientNormalParquetFile = "./TestData/Expected/expected_patient_normal_{0}.parquet";
    }
}
