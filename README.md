# FHIR Analytics Pipelines

> This project is currently under private preview and is governed by its private preview terms of use. You should access this project only if you are participating in the private preview, or are a Microsoft FTE.

FHIR objects are often nested JSON structures. While this nestedness helps capture detailed information in health care, it requires rectangularization before it can be used for machine learning and analytics, which work best with tabular data. **FHIR Analytics Pipelines** is an open source project with the goal to help build components and pipelines for rectangularizing and moving FHIR data from [Azure API for FHIR](https://azure.microsoft.com/en-us/services/azure-api-for-fhir/), and [FHIR server for Azure](https://github.com/microsoft/fhir-server) to analytical stores such as [CDM folder on ADLS Gen2](https://docs.microsoft.com/en-us/common-data-model/data-lake), [Azure Synapse](https://azure.microsoft.com/en-us/services/synapse-analytics/), [Azure SQL](https://azure.microsoft.com/en-us/services/sql-database/), and [Azure Machine Learning](https://azure.microsoft.com/en-us/services/machine-learning/).

The project currently has the following solutions:

1. [FHIR to CDM](docs/fhir-to-cdm.md) tool  to create an ADF pipeline for moving data from a FHIR server to a [CDM folder](https://docs.microsoft.com/en-us/common-data-model/data-lake) in Azure Data Lake Storage Gen 2.
1. [Instructions](docs/cdm-to-synapse.md) for creating pipeline in Synapse workspace to move data from CDM folder to Synapse SQL.

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
