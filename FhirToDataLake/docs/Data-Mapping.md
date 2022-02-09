# Data mapping from FHIR to Synapse
The Synapse sync pipeline fetches, converts, and saves FHIR data to the Azure storage in the [Apache Parquet format](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/read-parquet). The Apache Parquet format is an open source file format designed for efficient and performant flat columnar storage format of data compared to row based files like CSV or TSV files. For more information about its detailed storage strategy, see the [Apache Parquet](https://parquet.apache.org/documentation/latest/) documentation.

Azure Synapse Analytics supports [Parquet files](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/query-parquet-files) in Serverless SQL pool. PowerShell [Set-SynapseEnvironment.ps1](../scripts/Set-SynapseEnvironment.ps1) is provided to users to create default EXTERNAL TABLEs/VIEWs on Synapse. Refer to [CREATE EXTERNAL TABLE](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/create-use-external-tables) and [CREATE VIEW](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/create-use-views) for information about EXTERNAL TABLEs/VIEWs on Synapse Serverless SQL pool.

The information below describes how the raw FHIR JSON is converted to Parquet format and subsequently mapped to EXTERNAL TABLEs/VIEWs on Synapse Serverless SQL pool.

## FHIR JSON data to Parquet data

Parquet format supports nested and repeated structure. Theoretically, JSON data can be converted losslessly to Parquet format, but for stability and practical purposes, we designed piece schemas to transform FHIR raw JSON data into Parquet following the rules listed below. The detailed schemas are available in the [**data/schemas**](https://github.com/microsoft/FHIR-Analytics-Pipelines/tree/main/FhirToDataLake/data/schemas) directory.

Data saved in the Parquet format is unreadable by humans due to the way it's compressed and stored. The sample [Python script](#using-python-library-to-quickly-read-parquet-files) reads a Parquet file with specific columns for a patient resource.

Below is a JSON example that shows how the data is stored in Parquet.

### Wrap raw JSON fields into single string

First, wrap the following fields in raw JSON data into a single string in the Parquet file. You can still use [JSON functions](https://docs.microsoft.com/en-us/sql/t-sql/functions/json-functions-transact-sql?view=sql-server-ver15) to parse and analyze them with Synapse Serverless SQL pool. 

**Fields with depth greater than 3.**

Fields with depth greater than 3 will be wrapped into single JSON string and will be placed at the common root field.

Example:

```javascript
/* Raw FHIR Json data */
{
"resourceType": "Patient",
"id": "example",
"identifier": [{
    "type": {
        // Depth of "coding" is 3, its sub fields will be wrapped
        "coding": [{
            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
            "code": "MR" 
        }]
    }}]
}

/* Converted data in parquet file */
{
"resourceType": "Patient",
"id": "example",
"identifier": [{
    "type": {
        // Depth of "coding" is 3, its sub fields will be wrapped
        "coding": "[{\"system\": \"http://terminology.hl7.org/CodeSystem/v2-0203\",\"code\": \"MR\"}]"
    }}]
}
```
    
**Extensions**
   
[FHIR Extension](https://www.hl7.org/fhir/extensibility.html#Extension) properties in raw JSON data will be wrapped into a single string.

Example:

```javascript
/* Raw FHIR Json data */
{
"resourceType": "Patient",
"id": "example",
// Extension properties will be wrapped into single Json string
"extension": [{
    "url": "http://nema.org/fhir/extensions#0010:1010",
    "valueQuantity": {
    "value": 56,
    "unit": "Y"
}}]
}

/* Converted data in parquet file */
{
"resourceType": "Patient",
"id": "example",
// Extension properties will be wrapped into single Json string
"extension": "[{\"url\": \"http://nema.org/fhir/extensions#0010:1010\",\"valueQuantity\": {\"value\": 56,\"unit\": \"Y\"}}]"
}
```
   

### Exclude primitive extensions and inline resources

Extensions on primitive types usually begin with a underline prefix in the raw FHIR JSON data. Inline [Resources](https://www.hl7.org/fhir/resource.html#Resource) such as "contained" and "outcome" properties are flexiable FHIR resource data properties. These two kinds of fields are excluded in Parquet files.

```javascript
/* Raw FHIR Json data */
{
"resourceType": "Patient",
"id": "example",
"gender": "male",
// Primitive extensions will excluded
"_gender": {
    "extension": [{
        "url": "http://nema.org/examples/extensions#gender",
        "valueCoding": {
            "code": "M"
    }}]
},
// Inline resources will be excluded
"contained" : {...}
}

/* Converted data in parquet file */
{
"resourceType": "Patient",
"id": "example",
"gender": "male",
}
```

### Choice Types

From the recommandation on SQL-based projection of FHIR resources [Sql-On-Fhir](https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#choice-types), choice types (denoted as elementName[x]) are represented as an SQL ```STRUCT``` of the elementName, where that struct contains a child for each type of the choice. The code example below adds an additional level for choice type fields.  For more information, see [Resource Index](https://www.hl7.org/fhir/resourcelist.html) from HL7.

```javascript
/* Raw FHIR Json data */
{
"resourceType": "Patient",
"id": "example",
// Choice Type "deceased.boolean" 
"deceasedBoolean": false
}

/* Converted data in parquet file */
{
"resourceType": "Patient",
"id": "example",
// Choice Type "deceased.boolean"
"deceased" : {
    "boolean": false
}
}
```

## Query Parquet data on Synapse
The PowerShell script [Set-SynapseEnvironment.ps1](../scripts/Set-SynapseEnvironment.ps1) creates default EXTERNAL TABLEs and VIEWs on Synapse Serverless SQL pool. 

Below are general rules of the default TABLEs and VIEWs defintions.

### Definitons for EXTERNAL TABLE

Each EXTERNAL TABLE is linked to all Parquet files for a specific resource type. They have names such as ```"[fhir].[{resource type name}]"```. 

EXTERNAL TABLE can directly parse nested data in Parquet files, so nested data is expanded to the leaf field. But it can't parse repeated data in Parquet files, Synapse Serverless SQL pool automatically wraps repeated columns into single JSON string when they are queried.

1. Columns for leaf fields.

    ```sql
    [text.id] NVARCHAR(4000),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX)
    ```

    ```javascript
    {
        // "text" is nested field contains sub fields.
        "text": {
            "status": "generated",
            "div": "....."
        }
    }
    ```

2. Columns for repeated fields.
   
    ```sql
    [name] VARCHAR(MAX)
    ```

    ```javascript
    {
        // "name" is repeated field contains multiple name instance.
        "name": [
            {
            "use": "official",
            "family": "Chalmers",
            "given": [ "Peter", "James" ]
        }]
    }
    ```

### Definitions for VIEW

VIEWs are linked to repeated fields in first depth among Parquet files of all resource types. They have names such as ```"[fhir].[{resource type name}{property name}]"```.

Most of properties in FHIR data can be repeated in first depth (that is ```"name"``` in ```"Patient"```, ```"category"``` in ```"Observation"```), and they are wrapped into a single string when they're queired in EXTERNAL TABLEs. VIEWs provide a quick look for the detailed fields under those commonly used fields.

In VIEWs, nested data is expanded to the leaf field, and we define columns link to repeated root field, and let Synapse wrap them into a single string when querying.

## Others

#### Using Python library to quickly read parquet files

Apache provides a library called [PyArrow](https://arrow.apache.org/docs/python/index.html) that reads/writes Parquet files. Refer to the following steps to read and confirm your exported FHIR Parquet files.

1. Install [PyArrow](https://arrow.apache.org/docs/python/index.html) library.
   
    ```Powershell
    python -m pip install pyarrow
    ```

1. The sample Python script below will read the entire Parquet file, or it will read the Parquet file with specific columns for **"Patient"** resource.
    
    ```Python
    import pyarrow.parquet as pq

    if __name__ == '__main__':
        parquet_file_path = "{Your Patient parquet file path}"
        parquetFile = pq.ParquetFile(parquet_file_path)

        # Print parquet file metadata.
        print(parquetFile.metadata)  

        # Print parquet file schema.
        print(parquetFile.schema)

        print("-" * 20 + "Read table." + "-" * 20)
        table = parquetFile.read()

        # Convert parquet data into pandas dataframe and print.
        print(table.to_pandas())
        # Convert parquet data into python dict/list and print.
        print(table.to_pydict())

        # Read "resourceType", "id" and "text.div" columns.
        # "text.div" is a nested column in "text".
        print("-"*20 + "Read table with structure columns." + "-"*20)
        table_struct_column = parquetFile.read(columns=["resourceType", "id", "text.div"])
        print(table_struct_column.to_pandas())

        # Read "resourceType", "id" and "name.use" columns.
        # "name.use" is a repeated column in "name".
        print("-"*20 + "Read table with array columns." + "-"*20)
        table_array_column = parquetFile.read(columns=["resourceType", "id", "name.list.element.use"])
        print(table_array_column.to_pandas())
    ```
