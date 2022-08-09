#include "ParquetTestUtilities.h"
#include "ParquetLib.h"
using namespace std;

TEST (ParquetLib, RegisterVailidParquetSchema)
{
    ParquetWriter* writer = CreateParquetWriter();

    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    int schemaStatus = RegisterParquetSchema(writer, resourceType.data(), exampleSchema.data());
    EXPECT_EQ(0, schemaStatus);

    DestroyParquetWriter(writer);
}

TEST (ParquetLib, RegisterInvalidParquetSchema)
{
    ParquetWriter* writer = CreateParquetWriter();

    string resourceType = "Patient";
    string exampleSchema = "invalid json";
    int schemaStatus = RegisterParquetSchema(writer, resourceType.data(), exampleSchema.data());
    EXPECT_EQ(11001, schemaStatus);

    DestroyParquetWriter(writer);
}

TEST (ParquetLib, RegisterEmptyParquetSchema)
{
    ParquetWriter* writer = CreateParquetWriter();

    string resourceType = "Patient";
    string exampleSchema = "";
    int schemaStatus = RegisterParquetSchema(writer, resourceType.data(), exampleSchema.data());
    EXPECT_EQ(11001, schemaStatus);

    DestroyParquetWriter(writer);
}

TEST (ParquetLib, WriteExamplePatient)
{
    ParquetWriter* writer = CreateParquetWriter();

    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    int schemaStatus = RegisterParquetSchema(writer, resourceType.data(), exampleSchema.data());
    EXPECT_EQ(0, schemaStatus);
    
    char* error = new char[256];
    int status = ConvertJsonToParquet(writer, resourceType.c_str(), PatientData.c_str(), PatientData.size(), outputData, &outputLength, error);
    // Write success.
    EXPECT_EQ(0, status);
    EXPECT_TRUE(outputLength > 0);

    // parse output stream to table again, and check it.
    const auto buffer = std::make_shared<arrow::Buffer>(*outputData, outputLength);

    const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(buffer);
    check_table_fields_columns(table, get_patient_schema());
    const auto expected_table = get_expected_patient_table();
    EXPECT_TRUE(expected_table->Equals(*table));

    DestroyParquetWriter(writer);
    ReleaseUnmanagedData(outputData);
}