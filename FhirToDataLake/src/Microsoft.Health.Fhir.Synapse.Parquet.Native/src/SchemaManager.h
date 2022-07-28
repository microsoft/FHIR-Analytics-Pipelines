#pragma once
#include <arrow/api.h>
#include <json/json.h>
#include <iostream>
#include <set>
#include <vector>
#include "ErrorCodes.h"

using namespace std;

const string ElementNodeName = "element";
const set<string> FhirIntTypes { "positiveInt", "integer", "unsignedInt" };
const set<string> FhirDecimalTypes { "decimal", "number" };
const set<string> FhirBooleanTypes { "boolean" };

shared_ptr<arrow::Field> GenerateStructField(const string& fieldName, const Json::Value& node);
vector<shared_ptr<arrow::Field>> GenerateSchemaFields(const Json::Value& node);
bool LoadJson(const string& json, Json::Value* root);

class SchemaManager
{
    private:
        unordered_map<string, shared_ptr<arrow::Schema>> _schemaSet;
    
    public:

        int AddSchema(const string& schemaKey, const string& schemaJson);

        shared_ptr<arrow::Schema> GetSchema(const string& schemaKey);
};
