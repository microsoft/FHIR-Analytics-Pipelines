#include "ParquetTestUtilities.h"

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

std::shared_ptr<arrow::Table> parse_buffer_to_table(std::shared_ptr<arrow::Buffer> res, arrow::Compression::type compression)
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
void check_table_fields_columns(const std::shared_ptr<arrow::Table>& table, const std::shared_ptr<arrow::Schema>& schema, int64_t expected_num_rows)
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