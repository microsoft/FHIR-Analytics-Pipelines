# Set-SynapseEnvironment Syntax

``` PowerShell
Set-SynapseEnvironment
    [-SynapseWorkspaceName] <string>
    [-StorageName] <string>
    [[-Database] <string>, default: "fhirdb"]
    [[-Container] <string>, default: "fhir"]
    [[-ResultPath] <string>, default: "result"]
    [[-MasterKey] <string>, default: "FhirSynapseLink0!"]
    [[-DataSourceType] <string>, default: "FHIR"]
    [[-Concurrent] <int>, default: 15]
    [[-CustomizedSchemaImage] <string>, default: None]
```

|Parameter   | Description   |
|---|---|
| SynapseWorkspaceName | Name of Synapse workspace instance. |
| StorageName | Name of Storage Account where parquet files are stored. |
| Database | Name of database to be created on Synapse serverless SQL pool |
| Container | Name of container on storage where parquet files are stored. |
| ResultPath | Path to the parquet folder. |
| MasterKey | Master key that will be set in the created database. The database needs to have the master key, and then you can create EXTERNAL TABLEs and VIEWs on it. |
| DataSourceType | The source data type. "FHIR", "DICOM". |
| Concurrent | Max concurrent tasks number that will be used to upload place holder files and execute SQL scripts. |
| CustomizedSchemaImage | Customized schema image reference. Need to be provided when customized schema is enable. |