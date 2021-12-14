# Data mapping from FHIR to Synapse
The Synapse sync pipeline fetch, convert and save the FHIR data to Azure storage in [parquet format](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/read-parquet), which is an open source file format designed for efficient as well as performant flat columnar storage format of data compared to row based files like CSV or TSV files. Its detailed storage strategy can be found in this [document](https://parquet.apache.org/documentation/latest/).

Azure Synapse Analytics have well support for [read Parquet files](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/query-parquet-files) in serverless SQL pool, we provide PowerShell [Set-SynapseEnvironment.ps1](../scripts/Set-SynapseEnvironment.ps1) for users to create default EXTERNAL TABLEs/VIEWs on Synapse, you can also refer our default [SQL scripts](https://github.com/microsoft/FHIR-Analytics-Pipelines/tree/main/FhirToDataLake/scripts/sql/Resources) to create customized EXTERNAL TABLEs or VIEWs by themselves.

Below shows how we convert raw FHIR Json data to parquet format, and how do we define the default EXTERNAL TABLEs/VIEWs.

## 1. FHIR Json data to parquet data

Parquet format support nested and repeated structure, theoretically a json data can be lossless convert to parquet format, but for stability and practical, we designed a piece schemas to transform FHIR raw Json data into parquet with belowing rules. The detailed schemas can be found in ["**data/schemas**"](https://github.com/microsoft/FHIR-Analytics-Pipelines/tree/main/FhirToDataLake/data/schemas) directory.

Data saved in parquet format is human unreadable due to compressed and storage strategy, you can quickly confirm you FHIR parquet file refer this [section](#using-python-library-to-quickly-read-parquet-files).

Here we use virtual Json example to show how the data be stored in parquet.

### A. Wrap raw Json fields into single string

We will wrap some fields in raw Json data into single string in parquet file, you can still use [JSON functions](https://docs.microsoft.com/en-us/sql/t-sql/functions/json-functions-transact-sql?view=sql-server-ver15) to parse and analyze them on Synapse. 

1. **Fields with depth greater than 3.**

    Fields with depth greater than 3 will be wrapped into single Json string, and be placed at they common root field.

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
    
1. **Extensions.**
   
    FHIR [extensions](https://www.hl7.org/fhir/extensibility.html#Extension) properties in raw Json data will be wrapped into single Json string.
    
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
   

### B. Exclude primitive extensions and inline resources

Extensions on primitive types are usually begin with underline prefix in raw FHIR json data, and [line resources](https://www.hl7.org/fhir/resource.html#Resource) (E.g. "contained" and "outcome" properties.) are flexiable FHIR resource data properties, those 2 kind of fields will be excluded in parquet files.

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

### C. Choice Types

From the recommandation on SQL-based projection of FHIR resources [Sql-On-Fhir](https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#choice-types), choice types (denoted as elementName[x]), are represented as an SQL ```STRUCT``` of the elementName, where that struct contains a child for each type of the choice. We will add an additional level for choice types fields refer to [definitions](https://www.hl7.org/fhir/resourcelist.html) from HL7.

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

## 2. Query parquet data on Synapse
We provide PowerShell script [Set-SynapseEnvironment.ps1](../scripts/Set-SynapseEnvironment.ps1) to create default EXTERNAL TABLEs and VIEWs on Synapse serverless SQL pool. You can follow this [document](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/query-parquet-files) and refer to our default [SQL scripts](https://github.com/microsoft/FHIR-Analytics-Pipelines/tree/main/FhirToDataLake/scripts/sql/Resources)  to manually create customized TABLEs and VIEWs step by step.

Here we show the general rules of our default TABLEs and VIEWs defintions.

### A. Definitons for EXTERNAL TABLE

Each EXTERNAL TABLE linked to all parquet files for a specific resource type, they have names like ```"[fhir].[{resource type name}]"```. 

EXTERNAL TABLE can directly parse nested data in parquet files so we expand nested data till the leaf field, but it cannot handle parse repeated data in parquet files, we define columns link to repeated root field, it can automaticly wrap them into single Json string when be queried.

1. Columns for leaf fields.

    ```javascript
    {
        // "text" is nested field contains sub fields.
        "text": {
            "status": "generated",
            "div": "....."
        }
    }
    ```

    ```sql
    [text.id] NVARCHAR(4000),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX)
    ```

2. Columns for repeated fields.
   
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

    ```sql
    [name] VARCHAR(MAX)
    ```


Our default VIEWs help to analyze repeated fields in first depth.

### B. Definitions for VIEW

VIEWs are linked to repeated fields in first depth among parquet files of all resource types, they have names like ```"[fhir].[{resource type name}{property name}]"```.

Most of properties in FHIR data can be repeated in first depth (E.g. ```"name"``` in ```"Patient"```, ```"category"``` in ```"Observation"```) and they will be wrapped into single string when be quired in EXTERNAL TABLEs, VIEWs provide a quick look for the detailed fields under those common used fields.

In VIEWs, we also expand nested data till the leaf field, and we define columns link to repeated root field, let Synapse help use wrap them into single string when querying.

## 3. Others

#### Using Python library to quickly read parquet files
Apache provide [pyarrow](https://arrow.apache.org/docs/python/index.html) library to read/write parquet files, you can try it to quickly read and confirm your exported FHIR parquet files.

1. Install [pyarrow](https://arrow.apache.org/docs/python/index.html) library.
   
    ```Powershell
    python -m pip install pyarrow
    ```

1. Sample python script to read entire parquet file or read parquet file with specific columns for **"Patient"** resource.
    
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