## Moving data from CDM folder to Azure Synapse

### Automatic Method: Using Scripts

We provide the scripts to automate the process of building a pipeline from the CDM folder to a Synapse workspace.

#### Prerequisite
Have the data exported in a CDM format and stored in ADLS gen2 storage.

#### Create pipeline
1. Edit the configuration file by putting in custom values. A config file might look like this:
```json
{
  "ResourceGroup": "",
  "TemplateFilePath": "../Templates/cdmToSynapse.json",
  "TemplateParameters": {
    "DataFactoryName": "",
    "SynapseWorkspace": "",
    "DedicatedSqlPool": "",
    "AdlsAccountForCdm": "",
    "CdmRootLocation": "cdm",
    "StagingContainer": "adfstaging",
    "Entities": ["LocalPatient", "LocalPatientAddress"]
  }
}
```
2. Run the scripts with the configuration file: 
```powershell
.\DeployCdmToSynapsePipeline.ps1 -Config: config.json
```
3. Add data factory MI as SQL user into SQL DB. Here is a sample SQL script to create a user and an assign role:
```sql
CREATE USER [datafactory-name] FROM EXTERNAL PROVIDER
GO
EXEC sp_addrolemember db_owner, [datafactory-name]
GO
```
4. Ensure that your database master key is created on the Synapse SQL pool. For more details, read about how to [create a database master key](https://docs.microsoft.com/sql/t-sql/statements/create-master-key-transact-sql?view=sql-server-ver15). 

Additionally, if you want to create a trigger with the pipeline dependency, here is a guideline from Azure Data Factory:
[Create tumbling window trigger dependencies - Azure Data Factory | Microsoft Docs](https://docs.microsoft.com/en-us/azure/data-factory/tumbling-window-trigger-dependency)

### Manual Method: Using Azure Portal and Synapse Studio

#### Prerequisite
1. A Synapse workspace. [Create](https://ms.portal.azure.com/#create/Microsoft.Synapse) one if you need. 
2. Managed identity enabled on the Synapse workspace
3. The managed identity has a Synapse SQL Administrator role. Navigate to Synapse Studio >> Manage >> Access Control to verify. If needed, provide access.
4. Database master key is created on the Synapse SQL pool. Refer the [documentation](https://docs.microsoft.com/en-us/sql/t-sql/statements/create-master-key-transact-sql?view=sql-server-ver15). 
5. Pipelines are allowed to access SQL Pools. This is the default setting in Synapse.
![Pipelines allowed to access SQL pools](assets/synapse-pipeline-access-to-sql.png)

#### Create pipeline

1. Open Synapse Studio. Go to Integrate => + => Pipeline. Provide details.

![Synapse new pipeline](assets/synapse-new-pipeline.png)

2. Create source in Data Flow Activity. Choose "Common Data Model" for source type. Click _New_ to create a new Linked service.

![Synapse new source](assets/synapse-new-source.png)

3. Select the storage that contains the CDM folders with FHIR data. Use _Managed Identity_ authentication method. Provide _Storage Blob Data Reader_ access for the storage to the managed identity name. Test the connection.

![Synapse new linked service](assets/synapse-new-linked-service.png)

4. Under _Source options_ configure the _Root location_ to the CDM container name, and _Entity_ to the table name under CDM folder that you want to copy data from. If you enable debugging you can use Browse button to see the available entities.

![Synapse source options](assets/synapse-source-options.png)

5. Click on _Import Schema_ under _Projection_ tab. It will import the table schema from the CDM folder.

![Synapse import schema](assets/synapse-import-schema.png)

6. Create sink. 

![Synapse create new sink](assets/synapse-create-sink.png)

7. Click on _New_ against the dataset to create a new dataset of type _Azure Synapse Analytics_. Set properties, and provide name for a new Table.

![Synapse create dataset](assets/synapse-create-dataset.png)

8. Go to the dataflow activity and fill-in the Staging linked service under Settings tab.

![Synapse staging linked service](assets/synapse-dataflow-staging-linked-service.png)

9. Publish and trigger the pipeline. Once the pipeline has run successfully, you can see the data in the table by connecting SQL Server Management Studio.
