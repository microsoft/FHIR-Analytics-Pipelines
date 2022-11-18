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

TEST (ParquetLib, RegisterInvalidContentParquetSchema)
{
    ParquetWriter* writer = CreateParquetWriter();

    string resourceType = "Patient";
    string exampleSchema = "invalid json";
    int schemaStatus = RegisterParquetSchema(writer, resourceType.data(), exampleSchema.data());
    EXPECT_EQ(11001, schemaStatus);

    DestroyParquetWriter(writer);
}

TEST (ParquetLib, RegisterEmptyOrWhiteSpaceContentParquetSchema)
{
    ParquetWriter* writer = CreateParquetWriter();

    string resourceType = "Patient";
    string emptySchema = "";
    int schemaStatus = RegisterParquetSchema(writer, resourceType.data(), emptySchema.data());
    EXPECT_EQ(11001, schemaStatus);

    string whiteSpaceSchema = " ";
    schemaStatus = RegisterParquetSchema(writer, resourceType.data(), whiteSpaceSchema.data());
    EXPECT_EQ(11001, schemaStatus);

    schemaStatus = RegisterParquetSchema(writer, resourceType.data(), nullptr);
    EXPECT_EQ(11001, schemaStatus);

    DestroyParquetWriter(writer);
}

TEST (ParquetLib, RegisterEmptyOrWhiteSpaceKeyParquetSchema)
{
    ParquetWriter* writer = CreateParquetWriter();

    string emptyResourceType = "";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    int schemaStatus = RegisterParquetSchema(writer, emptyResourceType.data(), exampleSchema.data());
    EXPECT_EQ(11001, schemaStatus);

    string whiteSpaceResourceType = " ";
    schemaStatus = RegisterParquetSchema(writer, whiteSpaceResourceType.data(), exampleSchema.data());
    EXPECT_EQ(11001, schemaStatus);

    schemaStatus = RegisterParquetSchema(writer, nullptr, exampleSchema.data());
    EXPECT_EQ(11001, schemaStatus);
    DestroyParquetWriter(writer);
}

TEST (ParquetLib, WriteWithNullResourceType)
{
    ParquetWriter* writer = CreateParquetWriter();

    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    int schemaStatus = RegisterParquetSchema(writer, resourceType.data(), exampleSchema.data());
    EXPECT_EQ(0, schemaStatus);
    
    char* error = new char[256];
    int status = ConvertJsonToParquet(writer, nullptr, PatientData.c_str(), PatientData.size(), outputData, &outputLength, error);
    // Write success.
    EXPECT_EQ(11001, status);
    EXPECT_EQ(0, outputLength);
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
    TryReleaseUnmanagedData(outputData);
    EXPECT_EQ(nullptr, *outputData);
}
