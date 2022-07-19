#include <gtest/gtest.h>
#include <string>
#include <json/json.h>
#include "SchemaManager.h"

using namespace std;

TEST (LoadJsonTest, Valid)
{
    // valid json
    string json = "{\"a\":123}";
    Json::Value value;
    bool success = LoadJson(json, &value);
    EXPECT_EQ(value["a"], 123);
    EXPECT_TRUE(success);
}

TEST (LoadJsonTest, Empty)
{
    // empty json
    string json = "";
    Json::Value value;
    bool success = LoadJson(json, &value);
    EXPECT_TRUE(value.isNull());
    EXPECT_FALSE(success);
}

TEST (LoadJsonTest, Invalid)
{
    // invalid json
    string json = "Abcdefg";
    Json::Value value;
    bool success = LoadJson(json, &value);
    EXPECT_TRUE(value.isNull());
    EXPECT_FALSE(success);
}

TEST (SchemaTest, Valid)
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

TEST (SchemaTest, Empty)
{
    SchemaManager schemaManager;
    string empty = "";
    int status = schemaManager.AddSchema("Empty", empty);
    auto emptySchema = schemaManager.GetSchema("Empty");
    EXPECT_EQ(11001, status);
    EXPECT_TRUE(emptySchema == nullptr);
}

TEST (SchemaTest, Invalid)
{
    SchemaManager schemaManager;
    string broken = "abc";
    int status = schemaManager.AddSchema("Broken", broken);
    auto brokenSchema = schemaManager.GetSchema("Broken");
    EXPECT_EQ(11001, status);
    EXPECT_TRUE(brokenSchema == nullptr);
}