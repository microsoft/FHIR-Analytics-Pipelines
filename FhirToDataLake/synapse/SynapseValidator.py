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
parser.add_argument("--synapse_workspace", required=True, help="Name of Synapse workspace.")
parser.add_argument("--fhir_server_url", required=True, help="Fhir server url.")
parser.add_argument("--schema_directory", required=True, help="Schema directory path.")
parser.add_argument("--database", default='fhirdb', help="Name of SQL database.")
parser.add_argument("--sql_username", help="SQL username.")
parser.add_argument("--sql_password", help="SQL password.")
args = parser.parse_args()

sql_server_endpoint = args.synapse_workspace + "-ondemand.sql.azuresynapse.net"
database = args.database
schema_directory = args.schema_directory
fhir_server_base_url = args.fhir_server_url
sql_username = args.sql_username
sql_password = args.sql_password

# Get SQL username and password from environment variables if not given them in script paramaters
if (sql_username is None) or (sql_password is None):
    sql_username = os.getenv('SQL_USERNAME')
    sql_password = os.getenv('SQL_PASSWORD')

pyodbc.pooling = False


class FhirApiDataClient:
    headers = {'Accept': 'application/fhir+json', "Prefer": "respond-async"}
    start_datetime = "1970-01-01T00:00:00-00:00"

    def __init__(self, fhir_server_base_url):
        self.fhir_server_base_url = fhir_server_base_url

    def get_all_entries(self, resource_type):
        query_base_url = self.fhir_server_base_url + f"/{resource_type}?_lastUpdated=ge{FhirApiDataClient.start_datetime}&_lastUpdated=lt{datetime.today().strftime('%Y-%m-%dT%H:%M:%S-00:00')}&_sort=_lastUpdated&_count=1000"
        
        result = []
        query_url = query_base_url
        while query_url:
            response = requests.get(query_url, headers=FhirApiDataClient.headers)
            if response.status_code != requests.codes.ok:
                raise Exception(f"Get data from {query_url} failed: " + response.text)

            response_content = response.content
            response_object = json.loads(response_content.decode('utf-8'))
            if "entry" in response_object:
                result.extend(response_object["entry"])
                continuation_token = self.parse_continuation_token(response_object)
                if continuation_token:
                    query_url = query_base_url + f"&ct={continuation_token}"
                else:
                    query_url = None
            else:
                query_url = None
        return result

    @staticmethod
    def parse_continuation_token(response_content):
        for link_item in response_content["link"]:
            if link_item["relation"] == "next":
                parsed_url = urlparse.urlparse(link_item["url"])
                return urlparse.parse_qs(parsed_url.query)["ct"][0]
        return None


class SchemaManager:
    def __init__(self, schema_directory):
        self.schemas = SchemaManager.load_schemas(schema_directory)

    @staticmethod
    def load_schemas(schema_directory) -> dict:
        schemas = {}
        for schema_file in os.listdir(schema_directory):
            with open(os.path.join(schema_directory, schema_file)) as f:
                schema = json.load(f)
                schemas[schema["Type"]] = schema
        return schemas

    def generate_external_table_column_dicts(self):
        column_dicts = {}
        for resource_type, schema in self.schemas.items():
            column_dict = {}
            SchemaManager.parse_schema(schema, [], [], column_dict)
            column_dicts[resource_type] = column_dict
        return column_dicts

    def get_schema(self, resource_type):
        return self.schemas[resource_type]

    def get_all_resource_types(self):
        return list(self.schemas.keys())

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
        self.connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' \
                    + sql_server_endpoint + ';DATABASE=' + database + ';UID=' + sql_username + ';PWD=' + sql_password

    def get_data(self, resource_type):
        connection = pyodbc.connect(self.connection_string)
        sql = f"SELECT * FROM [fhir].[{resource_type}]"
        result_dataframe = pd.read_sql(sql=sql, con=connection)
        connection.close()
        return result_dataframe


class DataClient:
    resource_type_key = "resource_type"
    expected_data_key = "expected"
    queried_data_key = "queried"
    column_dict_key = "column_dict"

    def __init__(self, sql_client, fhir_api_client, column_dicts):
        self.sql_client = sql_client
        self.fhir_api_client = fhir_api_client
        self.column_dicts = column_dicts

    def fetch(self, resource_type):
        result = {
            DataClient.resource_type_key: resource_type,
            DataClient.queried_data_key: self.sql_client.get_data(resource_type),
            DataClient.expected_data_key: self.fhir_api_client.get_all_entries(resource_type),
            DataClient.column_dict_key: self.column_dicts[resource_type]
        }
        return result


if __name__ == "__main__":
    sys.stdout.flush()

    print('-> Executing script file: ', str(sys.argv[0]))
    print('-> Synapse SQL server endpoint: ', sql_server_endpoint)
    print('-> Database: ', database)
    print('-> Fhir server url: ', fhir_server_base_url)

    schema_manager = SchemaManager(schema_directory)
    resource_types = schema_manager.get_all_resource_types()

    fhir_api_client = FhirApiDataClient(fhir_server_base_url)
    sql_client = SqlServerClient(sql_server_endpoint, database, sql_username, sql_password)

    dataClient = DataClient(sql_client, fhir_api_client, schema_manager.generate_external_table_column_dicts())

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(dataClient.fetch, resource_type) for resource_type in resource_types]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            resource_type_name = result[DataClient.resource_type_key]
            expected_entries = result[DataClient.expected_data_key]
            queried_df = result[DataClient.queried_data_key]
            column_dict = result[DataClient.column_dict_key]
            print(f"Get {queried_df.shape[0]} resources from \"{resource_type_name}\" on Synapse, columns number is {queried_df.shape[1]}")

            if len(expected_entries) != queried_df.shape[0]:
                raise Exception(f"The resource number of \"{resource_type_name}\" is incorrect."
                                f"Expected resource number is {len(expected_entries)}, "
                                f"while resources been queried on Synapse is {queried_df.shape[0]}.")

            if len(column_dict) != queried_df.shape[1]:
                expected_columns = set(column_dict.keys())
                queried_columns = set(queried_df.columns.tolist())
                raise Exception(f"The columns number of \"{resource_type_name}\" is incorrect."
                                f"Expected column number is {len(column_dict)}, "
                                f"while column number been queried on Synapse is {queried_df.shape[1]}.")

    print(f"Validating {len(resource_types)} resource types, completes in {str(time.time() - start_time)} seconds.")
