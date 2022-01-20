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

sql_server_endpoint = args.SynapseWorkspaceName + "-ondemand.sql.azuresynapse.net"
database = args.Database
schema_collection_directory = args.SchemaCollectionDirectory
fhir_server_base_url = args.FhirServerUrl

sql_username = os.getenv('SQL_USERNAME')
sql_password = os.getenv('SQL_PASSWORD')

pyodbc.pooling = False


class FhirApiDataClient:
    headers = {'Accept': 'application/fhir+json', "Prefer": "respond-async"}
    start_datetime = "1970-01-01T00:00:00-00:00"

    def __init__(self, baseUrl):
        self.baseUrl = baseUrl

    def get_all_entries(self, resource_type):
        end_datetime = datetime.today().strftime('%Y-%m-%dT%H:%M:%S-00:00')
        url = self.baseUrl + f"/{resource_type}?_lastUpdated=ge{FhirApiDataClient.start_datetime}&_lastUpdated=lt{end_datetime}&_sort=_lastUpdated&_count=1000"

        response = requests.get(url, headers=FhirApiDataClient.headers)
        if response.status_code != requests.codes.ok:
            raise Exception(f"Get data from {url} failed: " + response.text)

        response_content = response.content
        response_object = json.loads(response_content.decode('utf-8'))
        if "entry" not in response_object:
            return []

        result = []
        result.extend(response_object["entry"])
        continuation_token = self.parse_continuation_token(response_object)

        while continuation_token:
            response = requests.get(url, headers=FhirApiDataClient.headers, params={"ct": continuation_token})
            if response.status_code != requests.codes.ok:
                raise Exception(f"Get data from {url} failed: " + response.text)

            response_content = response.content
            response_object = json.loads(response_content.decode('utf-8'))
            if "entry" not in response_object:
                continuation_token = None
            else:
                result.extend(response_object["entry"])
                continuation_token = self.parse_continuation_token(response_object)
        return result

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


class SqlServerClient:
    def __init__(self, sql_server_endpoint, database, sql_username, sql_password):
        self.connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' \
                    + sql_server_endpoint + ';DATABASE=' + database + ';UID=' + sql_username + ';PWD=' + sql_password

    def get_data(self, resource_type):
        connection = pyodbc.connect(self.connectionString)
        sql = f"SELECT * FROM [fhir].[{resource_type}]"
        data_df = pd.read_sql(sql=sql, con=connection)
        connection.close()
        return data_df


class DataClient:
    resourceType = "resourceType"
    expected = "expected"
    queried = "queried"
    field_dict = "field_mapping_dict"

    def __init__(self, sql_client, fhir_api_client, field_mapping_dicts):
        self.sql_client = sql_client
        self.fhir_api_client = fhir_api_client
        self.field_mapping_dicts = field_mapping_dicts

    def fetch(self, resource_type):
        result = {}
        result[DataClient.resourceType] = resource_type
        result[DataClient.queried] = self.sql_client.get_data(resource_type)
        result[DataClient.expected] = self.fhir_api_client.get_all_entries(resource_type)
        result[DataClient.field_dict] = self.field_mapping_dicts[resource_type]
        return result


if __name__ == "__main__":
    sys.stdout.flush()

    print('-> Executing script file: ', str(sys.argv[0]))
    print('-> SynapseServerEndpoint: ', sql_server_endpoint)
    print('-> Database: ', database)
    print('-> FhirServerUrl: ', fhir_server_base_url)

    schema_manager = SchemaManager(schema_collection_directory)
    resource_types = schema_manager.get_all_resource_types()

    fhir_api_client = FhirApiDataClient(fhir_server_base_url)
    sql_client = SqlServerClient(sql_server_endpoint, database, sql_username, sql_password)

    dataFactory = DataClient(sql_client, fhir_api_client, schema_manager.generate_field_mapping_dicts())

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(dataFactory.fetch, resource_type) for resource_type in resource_types]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            resource_type = result[DataClient.resourceType]
            expected_entries = result[DataClient.expected]
            queried_df = result[DataClient.queried]
            field_mapping_dict = result[DataClient.field_dict]
            print(f"Get {queried_df.shape[0]} resources from \"{resource_type}\", columns number is {queried_df.shape[1]}")

            if len(expected_entries) != queried_df.shape[0]:
                raise Exception(f"The resource number of \"{resource_type}\" is incorrect."
                                f"Expected resource number is {len(expected_entries)}, "
                                f"while rows number on Synapse is {queried_df.shape[0]}.")

            if len(field_mapping_dict) != queried_df.shape[1]:
                expected_columns = set(field_mapping_dict.keys())
                queried_columns = set(queried_df.columns.tolist())
                raise Exception(f"The columns number of \"{resource_type}\" is incorrect. "
                                f"Expected column number is {len(field_mapping_dict)}, "
                                f"while column number on Synapse is {queried_df.shape[1]}.")

    print(f"Validating {len(resource_types)} resource types, completes in {str(time.time() - start_time)} seconds.")
