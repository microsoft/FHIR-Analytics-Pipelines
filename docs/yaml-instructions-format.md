# YAML Instruction files

YAML instructions files allows you to specify how you want to convert FHIR resources to corresponding tabular format. These instructions are used for generating the CDM schema as well as pipelines for moving data from FHIR server to a CDM store. The YAML file relies heavily on the FHIR specification, and uses FHIRPath syntax at places. Relying on FHIR specification allows for a concise and powerful syntax.

You can specify what resources and elements you want to convert, what computed columns you want to create, and how you want to handle the cardinalities. For each selected resource a master table is created. Elements of a resource can be converted to columns in the master table, or in the related child tables.

YAML instructions consist of two yaml files.

1. Resources config file, which stores conversion specification at the resource level.
1. Properties group config file, which stores referenceable conversion specification for the complex data types.

## Resources config file

The following annotated example explains the format of the Resources config file. See [FHIR Patient](https://www.hl7.org/fhir/patient.html) specification if needed.

```yaml
# This file stores resource-level instructions for creating tables and columns. 
# The complex data types referenced here are converted to columns using the Properties group config file.
# All the complex data types used by the elements in the resource config file must be defined in the 
# Properties group config file. 

# Create a nested map for each resource of interest. One master table will be created per resource.
Patient:
 # If you want to create child table for a resource element having (0..*) relationship within the resource, 
 # add it to the unrollPath list. Each child table will have a Foreign key referencing the Master Resource
 # table using ResourceId. You can also use dot notation to create child tables for grandchildren.
 # For example, see contact.telecom below. Grandchildren have Foreign key reference to the main Resource 
 # table as well as its immediate parent.
 unrollPath: [identifier, telecom, address, contact, contact.telecom, generalPractitioner, managingOrganization, link]
 # If you want to convert an element as column in the master resource table, add it to the propertiesByDefault # list. Elements with complex data types are converted using the Properties group config file.
 # If the element has a (0..*) relationship with respect to the resource, only the first member will be 
 # stored. Based on the entry below, columns will be generated for only one set of Patient name and the  
 # first instance of the name will be stored in those columns.
 propertiesByDefault: [active, name, gender, birthDate, maritalStatus]
 # customProperties provide finer control to derive columns from the main resource. For each custom 
 # property, one column is added to the main table.
 customProperties:
  # Path uses dot notation and provides the scop for expression. Expression uses FHIRPath syntax. 
  # The text of first preferred communication languages will be stored in the CommunicationLanguageText 
  # column of the Patient table. By convention, we use camel casing for column names.
  - {name: CommunicationLanguageText, path: "", expression: "communication.where(preferred=true)[0].text", type: string}
  - {name: CommunicationLanguageCodingSystem, path: "", expression: "communication.where(preferred=true)[0].language.coding[0].system", type: string}
  - {name: CommunicationLanguageCodingCode, path: "", expression: "communication.where(preferred=true)[0].language.coding[0].code", type: string}
  # The choice type elements must be added as customProperties
  - {​​name: DeceasedBoolean, path: deceased, type: boolean}
  - {​​name: DeceasedDateTime, path: deceased, type: dateTime}
  - {name: MultipleBirthBoolean, path: multiplebirth, type: boolean}
  - {name: multipleBirthInteger, path: multipleBirth, type: integer}
Account:
 ...
Practitioner:
 ...
```

Based on the above instructions, nine tables will be created for the Patient resource. One master table for Patient, and eight child tables for identifier, telecom, address, contact, contact.telecom, generalPractitioner, managingOrganization, and link.

## Properties group config file

The following annotated example explains the format of the Resources config file. See [FHIR Patient](https://www.hl7.org/fhir/patient.html), and [FHIR Data Types](https://www.hl7.org/fhir/datatypes.html) specification if needed.

```yml
# The Properties group file stores instructions for mapping complex data types to columns. 
# Multiple elements in the Resources config file can refer to the same complex data type in this file.

# Create a nested map for each complex data type used in the Resource config file.
Address:
 # If you want to convert an element as columns, add it to the propertiesByDefault list. 
 # If the element has a complex data type, the mapping of that data type should also be defined in this file.
 # If the element has a (0..*) relationship with respect to the resource, only the first member will be 
 # stored. 
 propertiesByDefault: [use, text, city, district, state, postalCode, country]
 # customProperties provide finer control to derive columns. For each custom property, one column is added.
 customProperties:
  - {name: line1, expression: "line[0]", type: string}
  - {name: line2, expression: "line[1]", type: string}
  - {name: line3, expression: "line[2]", type: string}

CodeableConcept:
 propertiesByDefault: [coding, text]

Coding:
 propertiesByDefault: [system, code, display]

ContactPoint:
 propertiesByDefault: [system, value, use]
 
HumanName:
 propertiesByDefault: [text, family]
 customProperties:
  - {name: given, expression: "given[0]",type: string}
  - {name: prefix, expression: "prefix[0]",type: string}
  - {name: suffix, expression: "suffix[0]",type: string}

Identifier:
  propertiesByDefault: [use, type, system, value, assigner]

Patient_Contact:
 propertiesByDefault: [relationship, name, address, gender, organization]

Patient_Link:
 propertiesByDefault: [other, type]

Period:
 propertiesByDefault: [start, end]

Practitioner_Qualification:
 propertiesByDefault: [identifier, code, issuer]

Reference:
 propertiesByDefault: [reference, type]
```
