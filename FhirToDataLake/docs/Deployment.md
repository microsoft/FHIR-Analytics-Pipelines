### Preparations
To deploy and run the FHIR analytics pipeline agent, you will need a FHIR server instance (suggest testing with OSS instance without authentication. For paas instances, make sure you have premission to assign the Managed Identity authentication).

### Deployment
1. Go to [Custom deployment](https://ms.portal.azure.com/#create/Microsoft.Template) on Azure, them select **Build your own template in the editor**.
2. Use provided [template](../deploy/templates/FhirSynapsePipelineTemplate.json) to deploy an analytics pipeline.

![image](./assets/templateParameters.png)


Parameters:
- **App Name**: Azure function name.
- **Fhir Server Url**: Fhir service endpoint. Managed FHIR service is recommended.
- **Authentication**: whether to access Azure API for FHIR with managed identity authentication. Set false if you are using an OSS public FHIR server instance.
- **Fhir version**: Currently R4 is supported.
- **Data Start**: Start time stamp to export the data.
- **Data End**: End timestamp to export the data, leave it empty if you want to periodically export data in real time.
- **Container name**: Container in azure storage to store results. The Azure storage will be created by the ARM template. You can find the output after deploy completes.
- **Package url**: The build package of the agent.
- **App Insight Location**: You can find logs in the deployed application insight resource.

### Test
The agent will run automatically after deployment. You can find the data written to the target container on azure storage.
![blob result](./assets/ExportedData.png)

Now you can try run our [script](../scripts/Set-SynapseEnvironment.ps1) to set up your synapse workspace and explore the data. 
