#include <fstream>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <chrono>
#include "CppUnitTest.h"
#include "../../src/Microsoft.Health.Fhir.Synapse.Parquet.Native/Status.h"
#include "../../src/Microsoft.Health.Fhir.Synapse.Parquet.Native/ParquetConverter.h"

using namespace Microsoft::VisualStudio::CppUnitTestFramework;
using namespace Microsoft::Health::Fhir::Synapse::Parquet::Native;

namespace Microsoft
{
	namespace Health
	{
		namespace Fhir
		{
			namespace Synapse
			{
				namespace Parquet
				{
					namespace Native
					{
						namespace UnitTest
						{
							TEST_CLASS(parquet_tests)
							{
							private:
								std::string expected_data_dir = "../../TestData/Expected/";
								char* patient_data =
									R"({"resourceType":"Patient","id":"UnittTest","name":[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}],"gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}})";

								std::vector<std::shared_ptr<arrow::Field>> get_patient_fields() const
								{
									std::vector<std::shared_ptr<arrow::Field>> name_fields;
									name_fields.push_back(arrow::field("use", arrow::utf8()));
									name_fields.push_back(arrow::field("family", arrow::utf8()));
									name_fields.push_back(arrow::field("given", arrow::list(
										arrow::field("element", arrow::utf8())
									)));

									std::vector<std::shared_ptr<arrow::Field>> managing_organization_fields;
									managing_organization_fields.push_back(arrow::field("reference", arrow::utf8()));

									std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
									arrow_fields.push_back(arrow::field("resourceType", arrow::utf8()));
									arrow_fields.push_back(arrow::field("id", arrow::utf8()));
									arrow_fields.push_back(arrow::field("name", arrow::list(
										arrow::field("element", arrow::struct_(name_fields))
									)));

									arrow_fields.push_back(arrow::field("gender", arrow::utf8()));
									arrow_fields.push_back(arrow::field("birthDate", arrow::utf8()));
									arrow_fields.push_back(arrow::field("deceasedBoolean", arrow::boolean()));
									arrow_fields.push_back(arrow::field("managingOrganization", arrow::struct_(
										managing_organization_fields
									)));

									return arrow_fields;
								}

								std::shared_ptr<arrow::Schema> get_patient_schema() const
								{
									const auto arrow_fields = get_patient_fields();
									return arrow::schema(arrow_fields);
								}

								std::vector<std::shared_ptr<arrow::Field>> get_observation_fields() const
								{
									std::vector<std::shared_ptr<arrow::Field>> meta_fields;
									meta_fields.push_back(arrow::field("versionId", arrow::utf8()));
									meta_fields.push_back(arrow::field("lastUpdated", arrow::utf8()));

									std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
									arrow_fields.push_back(arrow::field("resourceType", arrow::utf8()));
									arrow_fields.push_back(arrow::field("id", arrow::utf8()));
									arrow_fields.push_back(arrow::field("meta", arrow::struct_(meta_fields)));
									arrow_fields.push_back(arrow::field("status", arrow::utf8()));
									arrow_fields.push_back(arrow::field("effectiveDateTime", arrow::utf8()));
									arrow_fields.push_back(arrow::field("issued", arrow::utf8()));

									return arrow_fields;
								}

								std::shared_ptr<arrow::Schema> get_observation_schema() const
								{
									const auto arrow_fields = get_observation_fields();
									return arrow::schema(arrow_fields);
								}

								std::shared_ptr<arrow::Table> get_expected_patient_table() const
								{
									auto arrow_fields = get_patient_fields();
									const auto schema = get_patient_schema();

									std::vector<std::shared_ptr<arrow::Array>> arrays;

									std::shared_ptr<arrow::Array> resource_type_array;
									auto status = arrow::ipc::internal::json::ArrayFromJSON(schema->field(0)->type(), R"(["Patient"])", &resource_type_array);
									Assert::IsTrue(status.ok());
									arrays.push_back(resource_type_array);

									std::shared_ptr<arrow::Array> id_array;
									status = arrow::ipc::internal::json::ArrayFromJSON(schema->field(1)->type(), R"(["UnittTest"])", &id_array);
									Assert::IsTrue(status.ok());
									arrays.push_back(id_array);

									std::shared_ptr<arrow::Array> name_array;
									status = arrow::ipc::internal::json::ArrayFromJSON(schema->field(2)->type(), R"([[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}]])", &name_array);
									Assert::IsTrue(status.ok());
									arrays.push_back(name_array);

									std::shared_ptr<arrow::Array> gender_array;
									status = arrow::ipc::internal::json::ArrayFromJSON(schema->field(3)->type(), R"(["male"])", &gender_array);
									Assert::IsTrue(status.ok());
									arrays.push_back(gender_array);

									std::shared_ptr<arrow::Array> birthDate_array;
									status = arrow::ipc::internal::json::ArrayFromJSON(schema->field(4)->type(), R"(["1974-12-25"])", &birthDate_array);
									Assert::IsTrue(status.ok());
									arrays.push_back(birthDate_array);

									std::shared_ptr<arrow::Array> deceasedBoolean_array;
									status = arrow::ipc::internal::json::ArrayFromJSON(schema->field(5)->type(), R"([false])", &deceasedBoolean_array);
									Assert::IsTrue(status.ok());
									arrays.push_back(deceasedBoolean_array);

									std::shared_ptr<arrow::Array> managingOrganization_array;
									status = arrow::ipc::internal::json::ArrayFromJSON(schema->field(6)->type(), R"([{"reference":"Organization / 1"}])", &managingOrganization_array);
									Assert::IsTrue(status.ok());
									arrays.push_back(managingOrganization_array);

									auto expected_table = arrow::Table::Make(schema, arrays);
									return expected_table;
								}

								static std::string read_file_to_buffer(const std::string& file_path)
								{
									std::ifstream bin_file(file_path, std::ios::binary);
									Assert::IsTrue(bin_file.good(), L"open file failed.");

									std::ostringstream ostream;
									ostream << bin_file.rdbuf();

									bin_file.close();
									return ostream.str();
								}

								static std::shared_ptr<arrow::Table> parse_buffer_to_table(std::shared_ptr<arrow::Buffer> res, arrow::Compression::type compression = arrow::Compression::SNAPPY)
								{
									const auto buffer_reader = std::make_shared<arrow::io::BufferReader>(res->data(), res->size());
									std::unique_ptr<parquet::arrow::FileReader> reader;
									PARQUET_THROW_NOT_OK(
										parquet::arrow::OpenFile(buffer_reader, arrow::default_memory_pool(), &reader));

									// check compression
									Assert::IsTrue(reader->parquet_reader()->RowGroup(0)->metadata()->ColumnChunk(0)->compression() == compression);

									std::shared_ptr<arrow::Table> table;
									PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

									return table;
								}

								// check table fields and columns
								static void check_table_fields_columns(const std::shared_ptr<arrow::Table>& table, const std::shared_ptr<arrow::Schema>& schema, int64_t expected_num_rows = 1)
								{
									Assert::IsTrue(table->ValidateFull().ok());

									Assert::AreEqual(expected_num_rows, table->num_rows());
									Assert::AreEqual(schema->num_fields(), table->num_columns());
									Assert::IsTrue(table->schema()->Equals(schema, true));

									for (int i = 0; i < table->num_columns(); i++)
									{
										Assert::IsTrue(schema->field(i)->Equals(table->field(i), true));
										Assert::AreEqual(expected_num_rows, table->column(i)->length());
										Assert::AreEqual(1, table->column(i)->num_chunks());
									}
								}

							public:

								TEST_METHOD(convert_patient_data_with_default_options_test)
								{
									const auto parquet_instance = std::make_unique<ParquetConverter>();

									const auto schema = get_patient_schema();
									std::shared_ptr<arrow::Buffer> res;
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data, strlen(patient_data), schema, res);

									Assert::IsTrue(status.IsOk());

									// check the output stream 
									const std::string actual_result(reinterpret_cast<char*>(const_cast<uint8_t*>(res->data())), res->size());
									const std::string expected_result = read_file_to_buffer(expected_data_dir + "expected_patient.parquet");
									Assert::AreEqual(expected_result, actual_result);

									// parse output stream to table again, and check it
									const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);

									check_table_fields_columns(table, schema);

									const auto expected_table = get_expected_patient_table();
									Assert::IsTrue(expected_table->Equals(*table));
								}

								TEST_METHOD(convert_patient_data_with_uncompression_test)
								{
									ArrowParquetOption option;
									option.Compression = arrow::Compression::UNCOMPRESSED;
									const auto parquet_instance = std::make_unique<ParquetConverter>(option);
									
									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data, strlen(patient_data), schema, res);

									Assert::IsTrue(status.IsOk());

									const auto buffer_reader = std::make_shared<arrow::io::BufferReader>(res->data(), res->size());
									std::unique_ptr<parquet::arrow::FileReader> reader;
									PARQUET_THROW_NOT_OK(
										parquet::arrow::OpenFile(buffer_reader, arrow::default_memory_pool(), &reader));

									// check compression
									Assert::IsTrue(reader->parquet_reader()->RowGroup(0)->metadata()->ColumnChunk(0)->compression() == arrow::Compression::UNCOMPRESSED);
								}

								TEST_METHOD(convert_patient_data_with_missing_fields_test)
								{
									const auto parquet_instance = std::make_unique<ParquetConverter>();
									
									const char* patient_data_missing_fields =
										R"({"resourceType":"Patient","gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}})";

									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data_missing_fields, strlen(patient_data_missing_fields), schema, res);

									Assert::IsTrue(status.IsOk());

									// parse output stream to table again, and check it
									const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);

									check_table_fields_columns(table, schema);

									// column 1 and column 2 are missing, so the value is null
									for (int i = 0; i < table->num_columns(); i++)
									{
										if (i == 1 || i == 2)
										{
											Assert::AreEqual(static_cast<int64_t>(1), table->column(i)->null_count());
										}
										else
										{
											Assert::AreEqual(static_cast<int64_t>(0), table->column(i)->null_count());
										}
									}
								}

								TEST_METHOD(convert_patient_data_with_unexpected_fields_ignore_test)
								{
									// unexpected fields are ignored by default configuration
									const auto parquet_instance = std::make_unique<ParquetConverter>();
									
									const char* patient_data_unexpected_fields =
										R"({"unexpectedFields":true, "resourceType":"Patient","id":"UnittTest","name":[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}],"gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}})";

									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data_unexpected_fields, strlen(patient_data_unexpected_fields), schema, res);

									Assert::IsTrue(status.IsOk());

									// parse output stream to table again, and check it
									const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);

									check_table_fields_columns(table, schema);

									const auto expected_table = get_expected_patient_table();
									Assert::IsTrue(expected_table->Equals(*table));
								}

								TEST_METHOD(convert_patient_data_with_unexpected_fields_infertype_test)
								{
									ArrowParquetOption option;
									option.UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::InferType;
									const auto parquet_instance = std::make_unique<ParquetConverter>(option);

									const char* patient_data_unexpected_fields =
										R"({"unexpectedFields":true, "resourceType":"Patient","id":"UnittTest","name":[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}],"gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}})";

									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data_unexpected_fields, strlen(patient_data_unexpected_fields), schema, res);

									Assert::IsTrue(status.IsOk());

									const auto buffer_reader = std::make_shared<arrow::io::BufferReader>(res->data(), res->size());
									std::unique_ptr<parquet::arrow::FileReader> reader;
									PARQUET_THROW_NOT_OK(
										parquet::arrow::OpenFile(buffer_reader, arrow::default_memory_pool(), &reader));

									// parse output stream to table again, and check it
									const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);

									Assert::IsTrue(table->ValidateFull().ok());

									Assert::AreEqual(static_cast<int64_t>(1), table->num_rows());
									Assert::AreEqual(schema->num_fields() + 1, table->num_columns());
									Assert::IsFalse(table->schema()->Equals(schema));

									for (int i = 0; i < schema->num_fields(); i++)
									{
										Assert::IsTrue(schema->field(i)->Equals(table->field(i), true));
										Assert::AreEqual(static_cast<int64_t>(1), table->column(i)->length());
										Assert::AreEqual(1, table->column(i)->num_chunks());
									}

									// check unexpected field
									Assert::AreEqual(std::string("unexpectedFields"), table->field(table->num_columns() - 1)->name());
									Assert::IsTrue(table->field(table->num_columns() - 1)->type()->id() == arrow::Type::BOOL);
								}

								TEST_METHOD(convert_patient_data_with_unexpected_fields_error_test)
								{
									ArrowParquetOption option;
									option.UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::Error;
									const auto parquet_instance = std::make_unique<ParquetConverter>(option);

									const char* patient_data_unexpected_fields =
										R"({"unexpectedFields":true, "resourceType":"Patient","id":"UnittTest","name":[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}],"gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}})";

									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data_unexpected_fields, strlen(patient_data_unexpected_fields), schema, res);

									Assert::IsFalse(status.IsOk());
									Assert::IsTrue(status.Code() == StatusCode::ReadToTableFailed);
									Assert::AreEqual("Invalid: JSON parse error: unexpected field", status.Message().c_str());
								}

								TEST_METHOD(unmatched_schema_test)
								{
									// unmatched schema, the default option for unexpected fields is ignore, so the patient's fields are ignored, and the values in schema is null 
									const auto parquet_instance = std::make_unique<ParquetConverter>();
									
									std::shared_ptr<arrow::Buffer> res;
									const auto other_schema = arrow::schema(
										{ arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
										 arrow::field("c", arrow::int64()), arrow::field("part", arrow::utf8()) });
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data, strlen(patient_data), other_schema, res);

									Assert::IsTrue(status.IsOk());

									// parse output stream to table again, and check it
									const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);

									check_table_fields_columns(table, other_schema);

									// the value is null
									for (int i = 0; i < table->num_columns(); i++)
									{
										Assert::AreEqual(static_cast<int64_t>(1), table->column(i)->null_count());
									}
								}

								TEST_METHOD(unmatched_schema_error_test)
								{
									ArrowParquetOption option;
									option.UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::Error;
									const auto parquet_instance = std::make_unique<ParquetConverter>(option);
									
									std::shared_ptr<arrow::Buffer> res;
									const auto schema = arrow::schema(
										{ arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
										 arrow::field("c", arrow::int64()), arrow::field("part", arrow::utf8()) });
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data, strlen(patient_data), schema, res);

									Assert::IsFalse(status.IsOk());
									Assert::IsTrue(status.Code() == StatusCode::ReadToTableFailed);
									Assert::AreEqual("Invalid: JSON parse error: unexpected field", status.Message().c_str());
								}

								TEST_METHOD(empty_data_test)
								{
									const auto parquet_instance = std::make_unique<ParquetConverter>();
									
									constexpr auto empty_data = "";
									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(empty_data, strlen(empty_data), schema, res);

									Assert::IsFalse(status.IsOk());
									Assert::IsTrue(status.Code() == StatusCode::ReadToTableFailed);
									Assert::AreEqual("Invalid: Empty JSON file", status.Message().c_str());
								}

								TEST_METHOD(multi_rows_test)
								{
									const auto parquet_instance = std::make_unique<ParquetConverter>();

									std::ifstream t("../../TestData/Patient.ndjson");
									std::stringstream ss;
									ss << t.rdbuf();
									std::string& tmp_str = ss.str();
									const char* data = tmp_str.c_str();
									auto size = tmp_str.length();
									
									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();

									const Status status = parquet_instance->ConvertToParquetBuffer(data, size, schema, res);

									Assert::IsTrue(status.IsOk());

									// parse output stream to table again, and check it
									const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);

									check_table_fields_columns(table, get_patient_schema(), 7);
								}

								BEGIN_TEST_METHOD_ATTRIBUTE(large_data_test)
									TEST_METHOD_ATTRIBUTE(L"LargeDataTakesLong", L"true")
								END_TEST_METHOD_ATTRIBUTE(large_data_test)
								TEST_METHOD(large_data_test)
								{
									const auto parquet_instance = std::make_unique<ParquetConverter>();

									std::ifstream t("../../TestData/large_data.ndjson");
									std::stringstream ss;

									// expand input file to get large data in memory
									for (int i = 0; i < 30; i++)
									{
										t.clear();
										t.seekg(0, std::ios::beg);
										ss << t.rdbuf();
									}

									std::string& tmp_str = ss.str();
									const char* data = tmp_str.c_str();
									auto size = tmp_str.length();

									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_observation_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(data, size, schema, res);

									Assert::IsTrue(status.IsOk());

									// parse output stream to table again, and check it
									const std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);

									Assert::IsTrue(table->ValidateFull().ok());

									Assert::AreEqual(static_cast<int64_t>(450000), table->num_rows());
									Assert::AreEqual(6, table->num_columns());
								}

								TEST_METHOD(error_format_data_test)
								{
									const auto parquet_instance = std::make_unique<ParquetConverter>();
									
									const char* error_format_data = "{\"resourceType\",\"Patient\"}";
									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(error_format_data, strlen(error_format_data), schema, res);

									Assert::IsFalse(status.IsOk());
									Assert::IsTrue(status.Code() == StatusCode::ReadToTableFailed);
									Assert::AreEqual("Invalid: JSON parse error: Missing a colon after a name of object member. in row 0", status.Message().c_str());
								}

								TEST_METHOD(resource_size_larger_block_size_test)
								{
									ArrowParquetOption option;
									// set the block size a small value, so the resource data is larger than block size 
									option.BlockSize = 32;
									const auto parquet_instance = std::make_unique<ParquetConverter>(option);
									
									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();
									const Status status = parquet_instance->ConvertToParquetBuffer(patient_data, strlen(patient_data), schema, res);

									Assert::IsFalse(status.IsOk());
									Assert::IsTrue(status.Code() == StatusCode::ReadToTableFailed);
									Assert::AreEqual("Invalid: straddling object straddles two block boundaries (try to increase block size?)", status.Message().c_str());
								}

								TEST_METHOD(newlines_in_data_test)
								{
									// newlines(if newlines_in_values is false) or json delimiter(if newlines_in_values is true) is used to generate completion json objects between blocks
									// json is parsed by block, if the block size is large, all the input will in a block, and newlines doesn't take effect
									const auto parquet_instance_default = std::make_unique<ParquetConverter>();

									const char* oneline_patient_data =
										R"({"resourceType":"Patient","id":"1","name":[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}],"gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}}{"resourceType":"Patient","id":"2","name":[{"use":"official","family":"Chalmers","given":["Peter","James"]},{"use":"usual","given":["Jim"]}],"gender":"male","birthDate":"1974-12-25","deceasedBoolean":false,"managingOrganization":{"reference":"Organization / 1"}})";
									
									const char* newlines_patient_data =
										"{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\n{\"resourceType\":\"Patient\",\"id\":\"2\",\"name\":[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\n";
									
									const char* newlines_in_object_patient_data =
										"{\"resourceType\":\"Patient\"\n\n,\"id\":\"1\",\"name\":\n[{\"use\":\"official\",\"family\"\n:\n\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\n";
									
									const char* newlines_in_string_patient_data =
										"{\"resourceType\":\"Pat\nient\"\n\n,\"id\":\"1\",\"name\":\n[{\"use\":\"official\",\"family\"\n:\n\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\n";
									
									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();

									// case 1: 
									// large block size, all the input data will in one block, so newlines doesn't take effect
									Assert::IsTrue(parquet_instance_default->ConvertToParquetBuffer(oneline_patient_data, strlen(oneline_patient_data), schema, res).IsOk());

									// all objects in one line
									std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);
									check_table_fields_columns(table, schema, 2);
									std::string str_1_1 = table->ToString();

									// ndjson, newlines between two objects
									Assert::IsTrue(parquet_instance_default->ConvertToParquetBuffer(newlines_patient_data, strlen(newlines_patient_data), schema, res).IsOk());
									table = parse_buffer_to_table(res);
									check_table_fields_columns(table, schema, 2);
									std::string str_2_1 = table->ToString();

									// newlines in object
									Assert::IsTrue(parquet_instance_default->ConvertToParquetBuffer(newlines_in_object_patient_data, strlen(newlines_in_object_patient_data), schema, res).IsOk());
									table = parse_buffer_to_table(res);
									check_table_fields_columns(table, schema, 1);
									std::string str_3_1 = table->ToString();

									// newlines in string isn't allowed
									Status status_1 = parquet_instance_default->ConvertToParquetBuffer(newlines_in_string_patient_data, strlen(newlines_in_string_patient_data), schema, res);
									Assert::IsFalse(status_1.IsOk());
									Assert::IsTrue(status_1.Code() == StatusCode::ReadToTableFailed);
									Assert::AreEqual("Invalid: JSON parse error", status_1.Message().substr(0, 25).c_str());

									// case 2:
									// the block size is larger than one resource size, while less than the whole input data size
									// only ndjson is supported
									ArrowParquetOption option;
									option.BlockSize = 200;
									const auto parquet_instance_custom = std::make_unique<ParquetConverter>(option);

									// all objects in one line isn't allowed
									Status status_3 = parquet_instance_custom->ConvertToParquetBuffer(oneline_patient_data, strlen(oneline_patient_data), schema, res);
									Assert::IsFalse(status_3.IsOk());
									Assert::IsTrue(status_3.Code() == StatusCode::ReadToTableFailed);
									// input error, fails to find the delimiter(newline) in two blocks, so arrow mistakenly thought the json object is larger than two blocks 
									Assert::AreEqual("Invalid: straddling object straddles two block boundaries (try to increase block size?)", status_3.Message().c_str());

									// ndjson, newlines between two objects
									Assert::IsTrue(parquet_instance_custom->ConvertToParquetBuffer(newlines_patient_data, strlen(newlines_patient_data), schema, res).IsOk());
									table = parse_buffer_to_table(res);
									check_table_fields_columns(table, schema, 2);
									std::string str_2_3 = table->ToString();
									Assert::AreEqual(str_2_3, str_2_1);

									// newlines in object isn't allowed, parse error
									Status status_4 = parquet_instance_custom->ConvertToParquetBuffer(newlines_in_object_patient_data, strlen(newlines_in_object_patient_data), schema, res);
									Assert::IsFalse(status_4.IsOk());
									Assert::IsTrue(status_4.Code() == StatusCode::ReadToTableFailed);
									// newline in json object isn't allowed, parse error
									Assert::AreEqual("Invalid: JSON parse error", status_4.Message().substr(0, 25).c_str());

									// newlines in string isn't allowed
									Status status_5 = parquet_instance_custom->ConvertToParquetBuffer(newlines_in_string_patient_data, strlen(newlines_in_string_patient_data), schema, res);
									Assert::IsFalse(status_5.IsOk());
									Assert::IsTrue(status_5.Code() == StatusCode::ReadToTableFailed);
									Assert::AreEqual("Invalid: JSON parse error", status_5.Message().substr(0, 25).c_str());
								}

								TEST_METHOD(newline_delimiters_test)
								{
									const char* newlines_patient_data_n =
										"{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\n{\"resourceType\":\"Patient\",\"id\":\"2\",\"name\":[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\n";
									
									const char* newlines_patient_data_r =
										"{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\r{\"resourceType\":\"Patient\",\"id\":\"2\",\"name\":[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\r";
									
									const char* newlines_patient_data_rn =
										"{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\r\n{\"resourceType\":\"Patient\",\"id\":\"2\",\"name\":[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]}],\"gender\":\"male\",\"birthDate\":\"1974-12-25\",\"deceasedBoolean\":false,\"managingOrganization\":{\"reference\":\"Organization / 1\"}}\r\n";
									
									std::shared_ptr<arrow::Buffer> res;
									const auto schema = get_patient_schema();

									// the block size is larger than one resource size, while less than the whole input data size
									ArrowParquetOption option;
									option.BlockSize = 200;
									const auto parquet_instance = std::make_unique<ParquetConverter>(option);

									// ndjson, '\n' as newline delimiter
									Assert::IsTrue(parquet_instance->ConvertToParquetBuffer(newlines_patient_data_n, strlen(newlines_patient_data_n), schema, res).IsOk());
									std::shared_ptr<arrow::Table> table = parse_buffer_to_table(res);
									check_table_fields_columns(table, schema, 2);
									std::string str_n = table->ToString();

									Assert::IsTrue(parquet_instance->ConvertToParquetBuffer(newlines_patient_data_r, strlen(newlines_patient_data_r), schema, res).IsOk());
									table = parse_buffer_to_table(res);
									check_table_fields_columns(table, schema, 2);
									std::string str_r = table->ToString();

									Assert::IsTrue(parquet_instance->ConvertToParquetBuffer(newlines_patient_data_rn, strlen(newlines_patient_data_rn), schema, res).IsOk());
									table = parse_buffer_to_table(res);
									check_table_fields_columns(table, schema, 2);
									std::string str_rn = table->ToString();

									Assert::AreEqual(str_n, str_r);
									Assert::AreEqual(str_n, str_rn);
								}
							};
						}
					}
				}
			}
		}
	}
}