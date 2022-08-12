# Generate parquet data for FHIR extensions

## 1.	Prepare the customized schema templates
For each resource types, E.g. Patient, Observation, you need to prepare a liquid template and a related JSON schema file.
The liquid template will be used to convert the raw FHIR JSON data into target structure.
The JSON schema will be used to generate the Parquet schema and create table definitions.
