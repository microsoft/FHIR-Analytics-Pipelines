#include "SchemaManager.h"

bool LoadJson(const string& json, Json::Value* root)
{
    const auto jsonLength = static_cast<int>(json.length());
    JSONCPP_STRING errorMessage;
    Json::CharReaderBuilder builder;
    const unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(json.c_str(), json.c_str() + jsonLength, root, &errorMessage))
    {
        return false;
    }

    return true;
}

shared_ptr<arrow::Field> GeneratePrimitiveField(const string& fieldName, const Json::Value& node)
{
    string dataType = node["Type"].asString();
    if (FhirIntTypes.find(dataType) != FhirIntTypes.end())
    {
        return arrow::field(fieldName, arrow::int32());
    }
    else if (FhirDecimalTypes.find(dataType) != FhirDecimalTypes.end())
    {
        return arrow::field(fieldName, arrow::float64());
    }
    else if (FhirBooleanTypes.find(dataType) != FhirBooleanTypes.end())
    {
        return arrow::field(fieldName, arrow::boolean());
    }
    
    // otherwise, it's string field.
    return arrow::field(fieldName, arrow::utf8());
}

shared_ptr<arrow::Field> GenerateListField(const string& fieldName, const Json::Value& node)
{
    bool isLeaf = node.get("IsLeaf", false).asBool();
    // List of primitive types like Patient.name.given
    if (isLeaf)
	{
        return arrow::field(fieldName, arrow::list(GeneratePrimitiveField(ElementNodeName, node)));
    }

    // List of struct types like Patient.name
    return arrow::field(fieldName, arrow::list(GenerateStructField(ElementNodeName, node)));
}

shared_ptr<arrow::Field> GenerateStructField(const string& fieldName, const Json::Value& node)
{
    return arrow::field(fieldName, arrow::struct_(GenerateSchemaFields(node)));
}

vector<shared_ptr<arrow::Field>> GenerateSchemaFields(const Json::Value& node)
{
    vector<shared_ptr<arrow::Field>> result;
    const Json::Value subNodes = node["SubNodes"];
    // SubNodes field not exists.
    if (subNodes.isNull())
    {
        return result;
    }

    for (auto const& fieldName : subNodes.getMemberNames())
    {
        const Json::Value subNode = subNodes[fieldName];
        
        if (subNode["IsRepeated"].asBool())
        {
            result.push_back(GenerateListField(fieldName, subNode));
        }
        else if (subNode["IsLeaf"].asBool())
        {
            result.push_back(GeneratePrimitiveField(fieldName, subNode));
        }
        else
        {
            result.push_back(GenerateStructField(fieldName, subNode));
        }
    }

    return result;
}

// Get schema from schema manager, will return nullptr if schemaKey not present.
shared_ptr<arrow::Schema> SchemaManager::GetSchema(const string& schemaKey)
{
    auto itr = _schemaSet.find(schemaKey);
    if (itr == _schemaSet.end())
    {
        // return null if not exists.
        return shared_ptr<arrow::Schema>();
    }
	
    return itr->second;
}

// Add schema to schema manager, will overwrite the schema if schemaKey already presents.
// Return 0 if operation succeeds.
int SchemaManager::AddSchema(const string& schemaKey, const string& schemaJson)
{
    if (isNullOrWhitespace(schemaKey) || isNullOrWhitespace(schemaJson))
    {
        return ParseParquetSchemaError;
    }

    Json::Value root;
    if (!LoadJson(schemaJson, &root))
    {
        return ParseParquetSchemaError;
    }

    try
    {
        _schemaSet[schemaKey] = arrow::schema(GenerateSchemaFields(root));
        return 0;
    }
    catch (const std::exception& e)
    {
        return ParseParquetSchemaError;
    }
}

bool isNullOrWhitespace(const std::string& str) {
    return str.empty()
        || std::all_of(str.begin(), str.end(), [](char c) {
        return std::isspace(static_cast<unsigned char>(c));
    });
}
