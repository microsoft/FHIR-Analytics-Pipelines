namespace Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch
{
    public class PatientWrapper
    {
        public PatientWrapper(
            string patientId,
            bool isNewPatient = true)
        {
            PatientId = patientId;
            IsNewPatient = isNewPatient;
        }

        public string PatientId { get; }

        public bool IsNewPatient { get; set; }
    }
}
