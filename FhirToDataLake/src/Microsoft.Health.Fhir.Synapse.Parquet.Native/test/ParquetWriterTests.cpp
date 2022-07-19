#include <gtest/gtest.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <string>
#include <fstream>
#include "ParquetWriter.h"

using namespace std;

string TestDataDir = string(std::getenv("TESTDATADIR"));
string ExpectedDataDir = TestDataDir + "Expected/";
string PatientData = R"({"resourceType":"Patient","id":"UnittTest","name":[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}],"gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}})";

std::vector<std::shared_ptr<arrow::Field>> get_patient_fields()
{
    std::vector<std::shared_ptr<arrow::Field>> name_fields;
    name_fields.push_back(arrow::field("family", arrow::utf8()));
    name_fields.push_back(arrow::field("given", arrow::list(
        arrow::field("element", arrow::utf8())
    )));
    name_fields.push_back(arrow::field("use", arrow::utf8()));

    std::vector<std::shared_ptr<arrow::Field>> managing_organization_fields;
    managing_organization_fields.push_back(arrow::field("reference", arrow::utf8()));

    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    arrow_fields.push_back(arrow::field("birthDate", arrow::utf8()));
    arrow_fields.push_back(arrow::field("deceasedBoolean", arrow::boolean()));

    arrow_fields.push_back(arrow::field("gender", arrow::utf8()));
    arrow_fields.push_back(arrow::field("id", arrow::utf8()));
    arrow_fields.push_back(arrow::field("managingOrganization", arrow::struct_(
        managing_organization_fields
    )));
    arrow_fields.push_back(arrow::field("name", arrow::list(
        arrow::field("element", arrow::struct_(name_fields))
    )));
    arrow_fields.push_back(arrow::field("resourceType", arrow::utf8()));

    return arrow_fields;
}

std::shared_ptr<arrow::Schema> get_patient_schema()
{
    const auto arrow_fields = get_patient_fields();
    return arrow::schema(arrow_fields);
}

std::shared_ptr<arrow::Table> get_expected_patient_table()
{
    auto arrow_fields = get_patient_fields();
    const auto schema = get_patient_schema();

    std::vector<std::shared_ptr<arrow::Array>> arrays;

    auto birthDate_array = arrow::ipc::internal::json::ArrayFromJSON(schema->field(0)->type(), R"(["1974-12-25"])");
    arrays.push_back(birthDate_array.ValueOrDie());

    auto deceasedBoolean_array = arrow::ipc::internal::json::ArrayFromJSON(schema->field(1)->type(), R"([false])");
    arrays.push_back(deceasedBoolean_array.ValueOrDie());

    auto gender_array = arrow::ipc::internal::json::ArrayFromJSON(schema->field(2)->type(), R"(["male"])");
    arrays.push_back(gender_array.ValueOrDie());

    auto id_array = arrow::ipc::internal::json::ArrayFromJSON(schema->field(3)->type(), R"(["UnittTest"])");
    arrays.push_back(id_array.ValueOrDie());

    auto managingOrganization_array = arrow::ipc::internal::json::ArrayFromJSON(schema->field(4)->type(), R"([{"reference":"Organization / 1"}])");
    arrays.push_back(managingOrganization_array.ValueOrDie());
 
    auto name_array = arrow::ipc::internal::json::ArrayFromJSON(schema->field(5)->type(), R"([[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}]])");
    arrays.push_back(name_array.ValueOrDie());

    auto resource_type_array = arrow::ipc::internal::json::ArrayFromJSON(schema->field(6)->type(), R"(["Patient"])");
    arrays.push_back(resource_type_array.ValueOrDie());

    auto expected_table = arrow::Table::Make(schema, arrays);
    return expected_table;
}

std::string read_file_to_buffer(const std::string& file_path)
{
    std::ifstream bin_file(file_path, std::ios::binary);

    std::ostringstream ostream;
    ostream << bin_file.rdbuf();

    bin_file.close();
    return ostream.str();
}

std::string read_file_text(const std::string& file_path)
{
    std::ifstream inFile(file_path.c_str());

    std::stringstream sstream;
    sstream << inFile.rdbuf();
    return sstream.str();
}

static std::shared_ptr<arrow::Table> parse_buffer_to_table(std::shared_ptr<arrow::Buffer> res, arrow::Compression::type compression = arrow::Compression::SNAPPY)
{
    const auto buffer_reader = std::make_shared<arrow::io::BufferReader>(res->data(), res->size());
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(
        parquet::arrow::OpenFile(buffer_reader, arrow::default_memory_pool(), &reader));

    // check compression
    EXPECT_TRUE(reader->parquet_reader()->RowGroup(0)->metadata()->ColumnChunk(0)->compression() == compression);

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    return table;
}

// check table fields and columns
static void check_table_fields_columns(const std::shared_ptr<arrow::Table>& table, const std::shared_ptr<arrow::Schema>& schema, int64_t expected_num_rows = 1)
{
    EXPECT_TRUE(table->ValidateFull().ok());

    EXPECT_EQ(expected_num_rows, table->num_rows());
    EXPECT_EQ(schema->num_fields(), table->num_columns());
    EXPECT_TRUE(table->schema()->Equals(schema, true));

    for (int i = 0; i < table->num_columns(); i++)
    {
        EXPECT_TRUE(schema->field(i)->Equals(table->field(i), true));
        EXPECT_EQ(expected_num_rows, table->column(i)->length());
        EXPECT_EQ(1, table->column(i)->num_chunks());
    }
}


TEST (ParquetWriter, RegisterVailidSchema)
{
    string resourceType = "Patient";
    string exampleSchema = read_file_text(TestDataDir + "patient_example_schema.json");
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(0, schemaStatus);
}

TEST (ParquetWriter, RegisterInvalidSchema)
{
    string resourceType = "Patient";
    string exampleSchema = "invalid json";
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(11001, schemaStatus);
}

TEST (ParquetWriter, RegisterEmptySchema)
{
    string resourceType = "Patient";
    string exampleSchema = "";
    ParquetWriter writer;
    int schemaStatus = writer.RegisterSchema(resourceType, exampleSchema);
    EXPECT_EQ(11001, schemaStatus);
}

TEST (ParquetWriter, WritePatientWithNoSchema)
{
    byte** outputData = new byte*[1];
    int outputLength;
    string resourceType = "Patient";
    ParquetWriter writer;

    int status = writer.Write(resourceType, PatientData.c_str(), PatientData.size(), outputData, &outputLength);
    // Write success.
    EXPECT_EQ(11002, status);
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
    int status = writer.Write(resourceType, PatientData.substr(0, patientLength).c_str(), patientLength, outputData, &outputLength);

    // Write fail with invalid json.
    EXPECT_EQ(10001, status);
    EXPECT_EQ(0, outputLength);
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
    int status = writer.Write(resourceType, emptyPatient.c_str(), 0, outputData, &outputLength);
    // Write success but output is 0
    EXPECT_EQ(10001, status);
    EXPECT_EQ(0, outputLength);
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

    int status = writer.Write(resourceType, PatientData.c_str(), PatientData.size(), outputData, &outputLength);
    // Write success.
    EXPECT_EQ(0, status);
    EXPECT_TRUE(outputLength > 0);

    // parse output stream to table again, and check it.
    const auto buffer = std::make_shared<arrow::Buffer>(*outputData, outputLength);

    const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(buffer);
    check_table_fields_columns(table, get_patient_schema());
    const auto expected_table = get_expected_patient_table();
    EXPECT_TRUE(expected_table->Equals(*table));
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
    int status = writer.Write(resourceType, batchPatientData.c_str(), batchPatientData.size(), outputData, &outputLength);
    // Write success.
    EXPECT_EQ(0, status);
    EXPECT_TRUE(outputLength > 0);
    
    // check the output stream 
    const std::string actual_result(reinterpret_cast<char*>(*outputData), outputLength);
    const std::string expected_result = read_file_to_buffer(ExpectedDataDir + "expected_patient.parquet");
    EXPECT_EQ(expected_result, actual_result);

    // parse output stream to table again, and check it.
    const auto buffer = std::make_shared<arrow::Buffer>(*outputData, outputLength);
    const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(buffer);
    EXPECT_EQ(7, table->num_rows());
}

