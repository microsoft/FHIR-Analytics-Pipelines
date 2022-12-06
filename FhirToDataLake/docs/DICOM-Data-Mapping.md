# Data mapping from DICOM metadata to Synapse
The Synapse sync pipeline fetches, converts, and saves DICOM metadata to the Azure storage in the [Apache Parquet format](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/read-parquet). The Apache Parquet format is an open source file format designed for efficient and performant flat columnar storage format of data compared to row based files like CSV or TSV files. For more information about its detailed storage strategy, see the [Apache Parquet](https://parquet.apache.org/docs/) documentation.

Azure Synapse Analytics supports [Parquet files](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/query-parquet-files) in Serverless SQL pool. PowerShell [Set-SynapseEnvironment.ps1](../scripts/Set-SynapseEnvironment.ps1) is provided to users to create default EXTERNAL TABLEs/VIEWs on Synapse. Refer to [CREATE EXTERNAL TABLE](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/create-use-external-tables) and [CREATE VIEW](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/create-use-views) for information about EXTERNAL TABLEs/VIEWs on Synapse Serverless SQL pool.

The information below describes how DICOM JSON metadata is converted to Parquet format and subsequently mapped to EXTERNAL TABLEs on Synapse Serverless SQL pool.

## DICOM JSON metadata to Parquet data

Parquet format supports nested and repeated structure. Theoretically, JSON data can be converted losslessly to Parquet format, but for stability and practical purposes, we designed schemas to transform DICOM JSON metadata into Parquet following the rules listed below. The detailed schemas are available in the [**data/schemas/dicom**](https://github.com/microsoft/FHIR-Analytics-Pipelines/tree/main/FhirToDataLake/data/schemas/dicom) directory.

In [DICOM JSON model object structure](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_F.2.2.html), each attribute object should contain "vr" - a string encoding the DICOM Value representation, and at most one of "Value", "BulkDataURI" or "InlineBinary". Attributes with "vr" set to "SQ" are excluded. "BulkDataURI" and "InlineBinary" are excluded. All data are saved as type string in Parquet files.

Data saved in the Parquet format is unreadable by humans due to the way it's compressed and stored. The sample [Python script](#using-python-library-to-quickly-read-parquet-files) reads a Parquet file with specific columns.

## Query Parquet data on Synapse
The PowerShell script [Set-SynapseEnvironment.ps1](../scripts/Set-SynapseEnvironment.ps1) creates default EXTERNAL TABLEs on Synapse Serverless SQL pool. Tables enabled for Synapse Serverless SQL pool can have a maximum limit of [1,020 columns](https://learn.microsoft.com/en-us/azure/synapse-analytics/synapse-link/synapse-link-for-sql-known-issues), but the number of [DICOM metadata attributes](https://dicom.nema.org/medical/dicom/current/output/chtml/part06/chapter_6.html) is far beyond that. The Powershell script selects some attributes as default columns. You can update the columns accordingly in [Dicom.sql](https://github.com/microsoft/FHIR-Analytics-Pipelines/tree/main/FhirToDataLake/scripts/sql/dicom/Dicom.sql).

The EXTERNAL TABLE is linked to all Parquet files for DICOM metadata. The table name is ```"[dicom].[Dicom]"```. 

EXTERNAL TABLE can't parse repeated data in Parquet files. Synapse Serverless SQL pool automatically wraps repeated columns into single JSON string when they are queried.

## Others

#### Using Python library to quickly read parquet files

Apache provides a library called [PyArrow](https://arrow.apache.org/docs/python/index.html) that reads/writes Parquet files. Refer to the following steps to read and confirm your exported FHIR Parquet files.

1. Install [PyArrow](https://arrow.apache.org/docs/python/index.html) library.
   
    ```Powershell
    python -m pip install pyarrow
    ```

1. The sample Python script below will read the entire Parquet file, or it will read the Parquet file with specific columns.
    
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

        # Read "PatientID", "StudyInstanceUID", "SeriesInstanceUID" and "SOPInstanceUID" columns.
        print("-"*20 + "Read table with specified columns." + "-"*20)
        table_column = parquetFile.read(columns=["PatientID", "StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID"])
        print(table_column.to_pandas())
    ```
