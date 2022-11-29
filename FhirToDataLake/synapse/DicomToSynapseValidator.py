import os
import sys
import argparse
import json
import requests
import pyodbc
import pandas as pd
import time

parser = argparse.ArgumentParser()
parser.add_argument("--synapse_workspace", required=True, help="Name of Synapse workspace.")
parser.add_argument("--dicom_server_url", required=True, help="Dicom server url.")
parser.add_argument("--dicom_server_access_token", help="Dicom server bearer access token.")
parser.add_argument("--schema_directory", required=True, help="Schema directory path.")
parser.add_argument("--database", default='dicom', help="Name of SQL database.")
parser.add_argument("--sql_username", help="SQL username.")
parser.add_argument("--sql_password", help="SQL password.")
args = parser.parse_args()

sql_server_endpoint = args.synapse_workspace + "-ondemand.sql.azuresynapse.net"
database = args.database
schema_directory = args.schema_directory
dicom_server_base_url = args.dicom_server_url
dicom_server_access_token = args.dicom_server_access_token
sql_username = args.sql_username
sql_password = args.sql_password

# Get SQL username and password from environment variables if not given them in script paramaters
if (sql_username is None) or (sql_password is None):
    sql_username = os.getenv('SQL_USERNAME')
    sql_password = os.getenv('SQL_PASSWORD')

expected_column_count = 101
pyodbc.pooling = False


class DicomApiDataClient:
    headers = {}
    limit = 100

    def __init__(self, dicom_server_base_url, api_version="v1", access_token=None):
        self.dicom_server_base_url = dicom_server_base_url
        self.api_version = api_version
        if access_token:
            self.headers['Authorization'] = 'Bearer {0}'.format(access_token)

    def get_metadata_count(self):
        latest_offset = self.get_latest_offset()
        metadata_count, offset = 0, 0
        while offset <= latest_offset:
            query_url = self.dicom_server_base_url + f"/{self.api_version}/changefeed?offset={offset}&limit={self.limit}&includeMetadata=true"
            response = requests.get(query_url, headers=self.headers)
            if response.status_code != requests.codes.ok:
                raise Exception(f"Get data from {query_url} failed: " + response.text)

            response_content = response.content
            response_object = json.loads(response_content.decode('utf-8'))

            for change_feed in response_object:
                if change_feed['action'] == "Create" and change_feed['state'] == "Current":
                    metadata_count += 1

            offset += self.limit
        return metadata_count

    def get_latest_offset(self):
        query_url = self.dicom_server_base_url + f"/{self.api_version}/changefeed/latest"
        response = requests.get(query_url, headers=self.headers)
        if response.status_code != requests.codes.ok:
            raise Exception(f"Get data from {query_url} failed: " + response.text)

        response_content = response.content
        response_object = json.loads(response_content.decode('utf-8'))
        return response_object['sequence']


class SqlServerClient:
    def __init__(self, sql_server_endpoint, database, sql_username, sql_password):
        self.connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' \
                                 + sql_server_endpoint + ';DATABASE=' + database + ';UID=' + sql_username + ';PWD=' + sql_password

    def get_data(self, resource_type):
        connection = pyodbc.connect(self.connection_string)
        sql = f"SELECT * FROM [dicom].[{resource_type}]"
        result_dataframe = pd.read_sql(sql=sql, con=connection)
        connection.close()
        return result_dataframe


if __name__ == "__main__":
    sys.stdout.flush()

    print('-> Executing script file: ', str(sys.argv[0]))
    print('-> Synapse SQL server endpoint: ', sql_server_endpoint)
    print('-> Database: ', database)
    print('-> Dicom server url: ', dicom_server_base_url)

    if dicom_server_access_token:
        dicom_api_client = DicomApiDataClient(dicom_server_base_url, access_token=dicom_server_access_token)
    else:
        dicom_api_client = DicomApiDataClient(dicom_server_base_url)
    sql_client = SqlServerClient(sql_server_endpoint, database, sql_username, sql_password)

    start_time = time.time()
    resource_type_name = 'dicom'
    queried_default_df = sql_client.get_data(resource_type_name)
    print(f"Get {queried_default_df.shape[0]} resources from \"{resource_type_name}\" on Synapse, columns number is {queried_default_df.shape[1]}")

    expected_entries_count = dicom_api_client.get_metadata_count()

    if expected_entries_count != queried_default_df.shape[0]:
        raise Exception(f"The resource number of \"{resource_type_name}\" is incorrect."
                        f"Expected resource number is {expected_entries_count}, "
                        f"while resources been queried on Synapse is {queried_default_df.shape[0]}.")

    if expected_column_count != queried_default_df.shape[1]:
        raise Exception(f"The columns number of \"{resource_type_name}\" is incorrect."
                        f"Expected column number is {expected_column_count}, "
                        f"while column number been queried on Synapse is {queried_default_df.shape[1]}.")

    print(f"Validate dicom data completes in {str(time.time() - start_time)} seconds.")
