using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions
{
    public class ParseJsonSchemaException : Exception
    {
        public ParseJsonSchemaException(string message)
            : base(message)
        {
        }

        public ParseJsonSchemaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
