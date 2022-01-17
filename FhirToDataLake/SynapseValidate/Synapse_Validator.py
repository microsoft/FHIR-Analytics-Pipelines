import os
import sys
import argparse
import concurrent.futures
import json
import requests
import pyodbc
import pandas as pd
import time
from urllib import parse as urlparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--SynapseWorkspaceName", help="Name of Synapse workspace.")
parser.add_argument("--FhirServerUrl", help="Fhir server url.")
parser.add_argument("--Database", help="Name of database.")
parser.add_argument("--SchemaCollectionDirectory", help="Schema collection directory path.")
args = parser.parse_args()

Sql_Server_Endpoint = args.SynapseWorkspaceName + "-ondemand.sql.azuresynapse.net"
Database = args.Database
Schema_Collection_Directory = args.SchemaCollectionDirectory
Fhir_Server_Base_Url = args.FhirServerUrl
Sql_Username = os.getenv('SQL_USERNAME')
Sql_Password = os.getenv('SQL_PASSWORD')

pyodbc.pooling = False

class RawDataSource:
    Headers = {'Accept': 'application/fhir+json', "Prefer": "respond-async"}
    Start_Datetime = "1970-01-01T00:00:00-00:00"

    def __init__(self, baseUrl):
        self.baseUrl = baseUrl

    def get_all_entries(self, resource_type):
        end_datetime = datetime.today().strftime('%Y-%m-%dT%H:%M:%S-00:00')
        url = self.baseUrl + f"/{resource_type}?_lastUpdated=ge{RawDataSource.Start_Datetime}&_lastUpdated=lt{end_datetime}&_sort=_lastUpdated&_count=1000"
        historical_data = self.get_entries(url)

        '''
        url = self.baseUrl + f"/{resource_type}?_lastUpdated=ge{end_datetime}&_lastUpdated=lt{end_datetime}&_sort=_lastUpdated&_count=1000"
        latest_data = self.get_entries(url)
        return historical_data + latest_data
        '''
        return historical_data


    def get_entries(self, url):
        response = requests.get(url, headers=RawDataSource.Headers)
        if response.status_code != requests.codes.ok:
            raise Exception(f"Get data from {url} failed: " + response.text)

        response_content = response.content
        response_object = json.loads(response_content.decode('utf-8'))
        if "entry" not in response_object:
            return []

        sumData = []
        sumData.extend(response_object["entry"])
        continuation_token = self.parse_continuation_token(response_object)

        while continuation_token:
            response = requests.get(url, headers=RawDataSource.Headers, params={"ct": continuation_token})
            if response.status_code != requests.codes.ok:
                raise Exception(f"Get data from {url} failed: " + response.text)

            response_content = response.content
            response_object = json.loads(response_content.decode('utf-8'))
            if "entry" not in response_object:
                continuation_token = None
            else:
                sumData.extend(response_object["entry"])
                continuation_token = self.parse_continuation_token(response_object)
        return sumData

    @staticmethod
    def parse_continuation_token(response_content):
        for link_item in response_content["link"]:
            if link_item["relation"] == "next":
                parsed_url = urlparse.urlparse(link_item["url"])
                return urlparse.parse_qs(parsed_url.query)["ct"][0]
        return None


class SchemaManager:
    def __init__(self, schema_collection_directory):
        self.schemas = SchemaManager.load_schemas(schema_collection_directory)

    @staticmethod
    def load_schemas(schema_collection_directory) -> dict:
        schemas = {}
        for schema_file in os.listdir(schema_collection_directory):
            with open(os.path.join(schema_collection_directory, schema_file)) as f:
                schema = json.load(f)
                schemas[schema["Type"]] = schema
        return schemas

    def generate_field_mapping_dicts(self):
        field_mapping_dicts = {}
        for resource_type, schema in self.schemas.items():
            field_mapping_dicts[resource_type] = SchemaManager.generate_external_table_field_mapping_dict(schema)
        return field_mapping_dicts

    def get_schema(self, resource_type):
        return self.schemas[resource_type]

    def get_all_resource_types(self):
        return list(self.schemas.keys())

    @staticmethod
    def generate_external_table_field_mapping_dict(schema):
        field_mapping_dict = {}
        SchemaManager.parse_schema(schema, [], [], field_mapping_dict)
        return field_mapping_dict

    @staticmethod
    def parse_schema(node_schema, json_data_path, table_column_path, field_dict_result):
        if node_schema["IsRepeated"]:
            field_dict_result[".".join(json_data_path)] = ".".join(table_column_path)
        elif node_schema["IsLeaf"]:
            field_dict_result[".".join(json_data_path)] = ".".join(table_column_path)
        else:
            for sub_node_name, sub_node_schema in node_schema["SubNodes"].items():
                table_column_path.append(sub_node_schema["Name"])
                json_data_path.append(sub_node_schema["Name"])

                SchemaManager.parse_schema(sub_node_schema, json_data_path, table_column_path, field_dict_result)

                table_column_path.pop()
                json_data_path.pop()
            return


class SQLServerClient:
    def __init__(self, sql_server_endpoint, database, sql_username, sql_password):
        self.connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' \
                    + sql_server_endpoint + ';DATABASE=' + database + ';UID=' + sql_username + ';PWD=' + sql_password

    def get_data(self, resource_type):
        connection = pyodbc.connect(self.connectionString)
        sql = f"SELECT * FROM [fhir].[{resource_type}]"
        data_df = pd.read_sql(sql=sql, con=connection)
        connection.close()
        return data_df


class DataFactory:
    ResourceType = "resourceType"
    Expected = "expected"
    Queried = "queried"
    FieldDict = "field_mapping_dict"

    def __init__(self, sql_client, expected_data_client, field_mapping_dicts):
        self.sql_client = sql_client
        self.expected_data_client = expected_data_client
        self.field_mapping_dicts = field_mapping_dicts

    def fetch(self, resource_type):
        result = {}
        result[DataFactory.ResourceType] = resource_type
        result[DataFactory.Queried] = self.sql_client.get_data(resource_type)
        result[DataFactory.Expected] = self.expected_data_client.get_all_entries(resource_type)
        result[DataFactory.FieldDict] = self.field_mapping_dicts[resource_type]
        return result


if __name__ == "__main__":
    sys.stdout.flush()

    print('-> Executing script file: ', str(sys.argv[0]))
    print('-> SynapseServerEndpoint: ', Sql_Server_Endpoint)
    print('-> Database: ', Database)
    print('-> FhirServerUrl: ', Fhir_Server_Base_Url)

    schema_manager = SchemaManager(Schema_Collection_Directory)
    resource_types = schema_manager.get_all_resource_types()

    data_source = RawDataSource(Fhir_Server_Base_Url)
    sql_client = SQLServerClient(Sql_Server_Endpoint, Database, Sql_Username, Sql_Password)

    dataFactory = DataFactory(sql_client, data_source, schema_manager.generate_field_mapping_dicts())

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(dataFactory.fetch, resource_type) for resource_type in resource_types]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            resource_type = result[DataFactory.ResourceType]
            expected_entries = result[DataFactory.Expected]
            queried_df = result[DataFactory.Queried]
            field_mapping_dict = result[DataFactory.FieldDict]
            print(f"Get {queried_df.shape[0]} resources from \"{resource_type}\", columns number is {queried_df.shape[1]}")

            if len(expected_entries) != queried_df.shape[0]:
                raise Exception(f"Resource \"{resource_type}\" rows number is incorrect. "
                                f"Expected resource number is {len(expected_entries)}, "
                                f"while rows number on Synapse is {queried_df.shape[0]}.")

            if len(field_mapping_dict) != queried_df.shape[1]:
                expected_columns = set(field_mapping_dict.keys())
                queried_columns = set(queried_df.columns.tolist())
                raise Exception(f"Resource \"{resource_type}\" columns number is incorrect. "
                                f"Expected column number is {len(field_mapping_dict)}, "
                                f"while column number number on Synapse is {queried_df.shape[1]}.")

    print(f"Validate {len(resource_types)} resource types complete in " + str(time.time() - start_time), "seconds")
