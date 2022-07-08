namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor
{
    public class ProcessParameters
    {
        public ProcessParameters(string schemaType, string resourceType)
        {
            SchemaType = schemaType;
            ResourceType = resourceType;
        }

        public string SchemaType { get; }

        public string ResourceType { get; }
    }
}
