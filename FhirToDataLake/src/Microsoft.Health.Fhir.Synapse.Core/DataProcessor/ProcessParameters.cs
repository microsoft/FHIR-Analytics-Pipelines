namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor
{
    public class ProcessParameters
    {
        public ProcessParameters(string schemaType)
        {
            SchemaType = schemaType;
        }

        public string SchemaType { get; }
    }
}
