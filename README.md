# Health Data Analytics Pipelines

Health Data Analytics Pipelines is an open source project with the goal to help build components and pipelines for transforming and moving FHIR and DICOM data from Azure Health Data Services(https://docs.microsoft.com/en-us/azure/healthcare-apis/), [Azure API for FHIR](https://azure.microsoft.com/en-us/services/azure-api-for-fhir/), [FHIR server for Azure](https://github.com/microsoft/fhir-server) or [DICOM server](https://github.com/microsoft/dicom-server) to [Azure Data Lake](https://azure.microsoft.com/en-us/solutions/data-lake/) and thereby make it available for analytics with [Azure Synapse](https://azure.microsoft.com/en-us/services/synapse-analytics/), [Power BI](https://powerbi.microsoft.com/en-us/), and [Azure Machine Learning](https://azure.microsoft.com/en-us/services/machine-learning/).

This OSS project currently has the following solutions:

1. [FHIR to Synapse sync agent](FhirToDataLake/docs/Deploy-FhirToDatalake.md): This is an [Azure Container App](https://learn.microsoft.com/en-us/azure/container-apps/?ocid=AID3042118) that extracts data from a FHIR server using FHIR Resource APIs, converts it to hierarchial Parquet files, and writes it to Azure Data Lake in near real time. This also contains a [script](FhirToDataLake/scripts/Set-SynapseEnvironment.ps1) to create external tables and views in Synapse Serverless SQL pool pointing to the Parquet files.

    This solution enables you to query against the entire FHIR data with tools such as Synapse Studio, SSMS, and Power BI. You can also access the Parquet files directly from a Synapse Spark pool. You should consider this solution if you want to access all of your FHIR data in near real time, and want to defer custom transformation to downstream systems.
    
1. [DICOM to Synapse sync agent](FhirToDataLake/docs/Deploy-DicomToDatalake.md): It is an [Azure Container App](https://learn.microsoft.com/en-us/azure/container-apps/?ocid=AID3042118) that extracts DICOM metadata from a DICOM server using DICOM [Change Feed](https://learn.microsoft.com/en-us/azure/healthcare-apis/dicom/dicom-change-feed-overview) APIs, converts it to hierarchical Parquet files, and writes it to Azure Data Lake in near real time. This solution also contains a script to create External Table in Synapse Serverless SQL pool pointing to the DICOM metadata Parquet files. For more information about DICOM External Tables, see [Data mapping from DICOM to Synapse](./DICOM-Data-Mapping.md).

    This solution enables you to query against the entire DICOM metadata with Synapse in SQL. You can also access the Parquet files directly from a Synapse Spark pool. 

1. [FHIR to CDM Pipeline Generator](FhirToCdm/docs/fhir-to-cdm.md): It is a tool to generate an ADF pipeline for moving a snapshot of data from a FHIR server using $export API to a [CDM folder](https://docs.microsoft.com/en-us/common-data-model/data-lake) in Azure Data Lake Storage Gen 2 in csv format. The tools requires a user-created configuration file containing instructions to project and flatten FHIR Resources and fields into tables. You can also follow the [instructions](FhirToCdm/docs/cdm-to-synapse.md) for creating a downstream pipeline in Synapse workspace to move data from CDM folder to Synapse dedicated SQL pool.

    This solution enables you to transform the data into tabular format as it gets written to CDM folder. You should consider this solution if you want to transform FHIR data into a custom schema as it is extracted from the FHIR server.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
