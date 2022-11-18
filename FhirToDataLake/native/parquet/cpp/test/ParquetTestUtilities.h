#pragma once
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <fstream>
#include <gtest/gtest.h>
#include <string>

using namespace std;

static string TestDataDir = string(std::getenv("TESTDATADIR"));
static string ExpectedDataDir = TestDataDir + "Expected/";
static string PatientData = R"({"resourceType":"Patient","id":"UnittTest","name":[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}],"gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}})";

std::vector<std::shared_ptr<arrow::Field>> get_patient_fields();
std::shared_ptr<arrow::Schema> get_patient_schema();
std::shared_ptr<arrow::Table> get_expected_patient_table();
std::string read_file_to_buffer(const std::string& file_path);
std::string read_file_text(const std::string& file_path);
std::shared_ptr<arrow::Table> parse_buffer_to_table(std::shared_ptr<arrow::Buffer> res, arrow::Compression::type compression = arrow::Compression::SNAPPY);
void check_table_fields_columns(const std::shared_ptr<arrow::Table>& table, const std::shared_ptr<arrow::Schema>& schema, int64_t expected_num_rows = 1);
