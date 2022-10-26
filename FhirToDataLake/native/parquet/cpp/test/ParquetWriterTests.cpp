#include "ParquetTestUtilities.h"
#include "ParquetWriter.h"
using namespace std;


TEST (ParquetWriter, RegisterVailidSchema)
{
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);
}

TEST (ParquetWriter, RegisterInvalidSchemaContent)
{
    string resourceType = "Patient";
    string invalidSchema = "invalid json";
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, invalidSchema);
    EXPECT_EQ(11001, schemaStatus);

    schemaStatus = writer.RegisterSchema(resourceType, "");
    EXPECT_EQ(11001, schemaStatus);

    schemaStatus = writer.RegisterSchema(resourceType, " ");
    EXPECT_EQ(11001, schemaStatus);
}

TEST (ParquetWriter, RegisterInvalidSchemaKey)
{
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema("", exampleSchema);
    EXPECT_EQ(11001, schemaStatus);

    schemaStatus = writer.RegisterSchema(" ", exampleSchema);
    EXPECT_EQ(11001, schemaStatus);
}

TEST (ParquetWriter, WritePatientWithNoSchema)
{
    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";
    ParquetWriter writer;

    char error[256];
    int status = writer.Write(resourceType, PatientData.c_str(), static_cast<int>(PatientData.size()), outputData, &outputLength, error);
    // Write success.
    EXPECT_EQ(11002, status);
    EXPECT_EQ(0, outputLength);
    EXPECT_EQ("Schema not found for '" + resourceType + "'.", std::string(error));
}

TEST (ParquetWriter, WriteInvalidPatient)
{
    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";

    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);

    int patientLength = 20;
    char error[256];
    int status = writer.Write(resourceType, PatientData.substr(0, patientLength).c_str(), patientLength, outputData, &outputLength, error);

    // Write fail with invalid json.
    EXPECT_EQ(10001, status);
    EXPECT_EQ(0, outputLength);
    EXPECT_EQ("Invalid: JSON parse error: Missing a closing quotation mark in string. in row 0", std::string(error));
}

TEST (ParquetWriter, WriteEmptyPatient)
{
    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";

    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);

    string emptyPatient = "";
    char error[256];
    int status = writer.Write(resourceType, emptyPatient.c_str(), 0, outputData, &outputLength, error);
    // Write success but output is 0
    EXPECT_EQ(10001, status);
    EXPECT_EQ(0, outputLength);
    EXPECT_EQ("Invalid: Empty JSON file", std::string(error));
}

TEST (ParquetWriter, WriteEmptyPatientWithoutError)
{
    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";

    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);

    string emptyPatient = "";
    int status = writer.Write(resourceType, emptyPatient.c_str(), 0, outputData, &outputLength);
    
    // Write success but output is 0
    EXPECT_EQ(10001, status);
    EXPECT_EQ(0, outputLength);
}

TEST (ParquetWriter, WriteNullPatient)
{
    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);

    char error[256];
    int status = writer.Write(resourceType, nullptr, static_cast<int>(PatientData.size()), outputData, &outputLength, error);
    // Write success.
    EXPECT_EQ(10001, status);
    EXPECT_EQ(0, outputLength);
    EXPECT_EQ("Input Json data is null.", std::string(error));
}

TEST (ParquetWriter, WriteWithNullOutputPointer)
{
    int outputLength = 0;
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);

    char error[256];
    int status = writer.Write(resourceType , PatientData.c_str(), static_cast<int>(PatientData.size()), nullptr, &outputLength, error);
    // Write success.
    EXPECT_EQ(10002, status);
    EXPECT_EQ(0, outputLength);
    EXPECT_EQ("Output data pointer is null.", std::string(error));
}

TEST (ParquetWriter, WriteWithNullOutputSize)
{
    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);

    char error[256];
    int status = writer.Write(resourceType , PatientData.c_str(), static_cast<int>(PatientData.size()), outputData, nullptr, error);
    // Write success.
    EXPECT_EQ(10002, status);
    EXPECT_EQ(0, outputLength);
    EXPECT_EQ("Output data size pointer is null.", std::string(error));
}

TEST (ParquetWriter, WriteExamplePatient)
{
    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);

    char error[256];
    int status = writer.Write(resourceType, PatientData.c_str(), static_cast<int>(PatientData.size()), outputData, &outputLength, error);
    // Write success.
    EXPECT_EQ(0, status);
    EXPECT_TRUE(outputLength > 0);
    EXPECT_EQ("", std::string(error));
    
    // parse output stream to table again, and check it.
    const auto buffer = std::make_shared<arrow::Buffer>(*outputData, outputLength);

    const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(buffer);
    check_table_fields_columns(table, get_patient_schema());
    const auto expected_table = get_expected_patient_table();
    EXPECT_TRUE(expected_table->Equals(*table));

    delete *outputData;
}

TEST (ParquetWriter, WriteBatchPatient)
{
    byte** outputData = new byte*[1];
    int outputLength = 0;
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);

    string batchPatientData = read_file_text(TestDataDir + "Patient.ndjson");
    char error[256];
    int status = writer.Write(resourceType, batchPatientData.c_str(), static_cast<int>(batchPatientData.size()), outputData, &outputLength, error);
    // Write success.
    EXPECT_EQ(0, status);
    EXPECT_TRUE(outputLength > 0);
    EXPECT_EQ("", std::string(error));

    // check the output stream 
    const std::string actual_result(reinterpret_cast<char*>(*outputData), outputLength);
    const std::string expected_result = read_file_to_buffer(ExpectedDataDir + "expected_patient.parquet");
    EXPECT_EQ(expected_result, actual_result);

    // parse output stream to table again, and check it.
    const auto buffer = std::make_shared<arrow::Buffer>(*outputData, outputLength);
    const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(buffer);
    EXPECT_EQ(7, table->num_rows());

    delete *outputData;
}
