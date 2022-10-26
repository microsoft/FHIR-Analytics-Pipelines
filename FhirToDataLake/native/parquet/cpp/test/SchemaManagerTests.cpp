#include <gtest/gtest.h>
#include <string>
#include <json/json.h>
#include "SchemaManager.h"

using namespace std;

TEST (LoadJsonTest, ValidContent)
{
    // valid json
    string json = "{\"a\":123}";
    Json::Value value;
    bool success = LoadJson(json, &value);
    EXPECT_EQ(value["a"], 123);
    EXPECT_TRUE(success);
}

TEST (LoadJsonTest, EmptyValidContent)
{
    // empty json
    string json = "";
    Json::Value value;
    bool success = LoadJson(json, &value);
    EXPECT_TRUE(value.isNull());
    EXPECT_FALSE(success);
}

TEST (LoadJsonTest, InvalidValidContent)
{
    // invalid json
    string json = "Abcdefg";
    Json::Value value;
    bool success = LoadJson(json, &value);
    EXPECT_TRUE(value.isNull());
    EXPECT_FALSE(success);
}

TEST (SchemaTest, AddAndGetValidSchema)
{
    SchemaManager schemaManager;
    string mockSchema = "{\"Name\": \"Organization\", \"NodePaths\": [\"Organization\"], \"SubNodes\": { \"id\": {\"Name\":\"id\", \"Depth\": 1, \"Type\": \"id\", \"IsLeaf\": true, \"IsRepeated\": false}}, \"Type\": \"Organization\", \"IsRepeated\": false}";
    int status = schemaManager.AddSchema("Organization", mockSchema);
    EXPECT_EQ(0, status);

    auto schema = schemaManager.GetSchema("Organization");
    auto schemaResult = schema->ToString();
    EXPECT_EQ(schemaResult, "id: string");
    EXPECT_EQ(1, schema->num_fields());
}

TEST (SchemaTest, AddAndGetEmptySchemaContent)
{
    SchemaManager schemaManager;
    string schemaKey = "Empty";
    string empty = "";
    int status = schemaManager.AddSchema(schemaKey, empty);
    EXPECT_EQ(11001, status);

    auto emptySchema = schemaManager.GetSchema(schemaKey);
    EXPECT_TRUE(emptySchema == nullptr);
}

TEST (SchemaTest,  AddAndGetNoneJsonSchemaContent)
{
    SchemaManager schemaManager;
    string schemaKey = "Broken";
    string broken = "abc";
    int status = schemaManager.AddSchema(schemaKey, broken);
    EXPECT_EQ(11001, status);

    auto brokenSchema = schemaManager.GetSchema(schemaKey);
    EXPECT_TRUE(brokenSchema == nullptr);
}

TEST (SchemaTest,  AddAndGetEmptyOrWhiteSpaceSchemaKey)
{
    SchemaManager schemaManager;
    string mockSchema = "{\"Name\": \"Organization\", \"NodePaths\": [\"Organization\"], \"SubNodes\": { \"id\": {\"Name\":\"id\", \"Depth\": 1, \"Type\": \"id\", \"IsLeaf\": true, \"IsRepeated\": false}}, \"Type\": \"Organization\", \"IsRepeated\": false}";
    int status = schemaManager.AddSchema("", mockSchema);
    EXPECT_EQ(11001, status);

    status = schemaManager.AddSchema(" ", mockSchema);
    EXPECT_EQ(11001, status);
}


TEST (SchemaTest,  CheckStringIsEmptyOrWhiteSpace)
{
    EXPECT_TRUE(IsEmptyOrWhitespace(string("")));
    EXPECT_TRUE(IsEmptyOrWhitespace(string(" ")));
    EXPECT_FALSE(IsEmptyOrWhitespace(string("a")));
    EXPECT_FALSE(IsEmptyOrWhitespace(string("ab")));
    EXPECT_FALSE(IsEmptyOrWhitespace(string(" a")));
    EXPECT_FALSE(IsEmptyOrWhitespace(string("a ")));
    EXPECT_FALSE(IsEmptyOrWhitespace(string(" aa ")));
}
