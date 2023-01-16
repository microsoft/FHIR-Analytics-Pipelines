Here we share some sample liquid templates to handle extensions.

### Sample Data
```json
{
    "resourceType": "Patient",
    "id": "123",
    "extension": [
        {
            "url": "http://hl7.org/fhir/StructureDefinition/individual-genderIdentity",
            "extension": [
                {
                    "url": "value",
                    "valueCodeableConcept" : {
                        "coding" : [{
                        "system" : "http://terminology.hl7.org/CodeSystem/sex-for-clinical-use",
                        "code" : "specified",
                        "display" : "Specified sex for clinical use"
                        }]
                    }
                },
                {
                    "url": "period",
                    "valuePeriod": {
                        "start": "2001-05-01"
                    }
                }
            ]
        },
        {
            "url": "http://hl7.org/fhir/StructureDefinition/individual-pronouns",
            "extension": [
                {
                    "url": "value",
                    "valueCodeableConcept" : {
                        "coding" : [{
                        "system" : "http://loinc.org",
                        "code" : "LA29519-8",
                        "display" : "she/her/her/hers/herself"
                        }]
                    }
                },
                {
                    "url" : "period",
                    "valuePeriod" : {
                        "start" : "2001-05-06"
                    }
                },
                {
                    "url" : "comment",
                    "valueString" : "Patient transitioned from male to female in 2001."
                }
            ]
        },
        {
            "url" : "comment",
            "valueString" : "Patient transitioned from male to female in 2001, but their driver's license still indicates male."
        }        
    ]
}
```

### Sample liquid template to process the extension content:
```
{% validate "Schema/Patient.schema.json" -%} 
  {
    "resourceType": "{{ msg.resourceType }}",
    "id": "{{ msg.id }}",

    {% assign individual_gender_identity_extension = msg.extension | where: "url", "http://hl7.org/fhir/StructureDefinition/individual-genderIdentity" | first -%}
    {% assign individual_gender_identity_value_extension = individual_gender_identity_extension.extension | where: "url": "value" | first -%}
    {% assign individual_gender_identity_value_extension_coding = individual_gender_identity_value_extension.valueCodeableConcept.coding | where: "system": "http://terminology.hl7.org/CodeSystem/sex-for-clinical-use" | first -%}
    "individual_gender_identity_value_coding_system": "{{individual_gender_identity_value_extension_coding.system}}",
    "individual_gender_identity_value_coding_code": "{{individual_gender_identity_value_extension_coding.code}}",
    "individual_gender_identity_value_coding_display": "{{individual_gender_identity_value_extension_coding.display}}",
    
    {% assign individual_gender_identity_period_extension = individual_gender_identity_extension.extension | where: "url": "period" | first -%}
    "individual_gender_identity_extension_period_start": "{{ individual_gender_identity_period_extension.valuePeriod.start }}",
    "individual_gender_identity_extension_period_end": "{{ individual_gender_identity_period_extension.valuePeriod.end }}",

    {% assign individual_pronouns_extension = msg.extension | where: "url", "http://hl7.org/fhir/StructureDefinition/individual-pronouns" | first -%}
    {% assign individual_pronouns_value_extension = individual_pronouns_extension.extension | where: "url": "value" | first -%}
    {% assign individual_pronouns_value_extension_coding = individual_pronouns_value_extension.valueCodeableConcept.coding | where: "system": "http://loinc.org" | first -%}
    "individual_pronouns_value_coding_system": "{{individual_pronouns_value_extension_coding.system}}",
    "individual_pronouns_value_coding_code": "{{individual_pronouns_value_extension_coding.code}}",
    "individual_pronouns_value_coding_display": "{{individual_pronouns_value_extension_coding.display}}",

    {% assign individual_pronouns_period_extension = individual_pronouns_extension.extension | where: "url": "period" | first -%}
    "individual_pronouns_extension_period_start": "{{ individual_pronouns_period_extension.valuePeriod.start }}",
    "individual_pronouns_extension_period_end": "{{ individual_pronouns_period_extension.valuePeriod.end }}",

    {% assign individual_pronouns_comment_extension = individual_pronouns_extension.extension | where: "url": "comment" | first -%}
    "individual_pronouns_extension_comment": "{{ individual_pronouns_comment_extension.valueString }}",

    {% assign comment_extension = msg.extension | where: "url", "comment" | first -%}
    "comment": "{{ comment_extension.valueString }}"
  }
{% endvalidate -%}
```

### Sample schema file
```json
{
    "title": "Sample extension schema for Patient",
    "type": "object",
    "properties": {
        "resourceType": { "type": "string" },
        "id": { "type": "string" },
        "individual_gender_identity_value_coding_system": { "type": "string" },
        "individual_gender_identity_value_coding_code": { "type": "string" },
        "individual_gender_identity_value_coding_display": { "type": "string" },
        "individual_gender_identity_extension_period_start": { "type": "string" },
        "individual_pronouns_value_coding_system": { "type": "string" },
        "individual_pronouns_value_coding_code": { "type": "string" },
        "individual_pronouns_value_coding_display": { "type": "string" },
        "individual_pronouns_extension_period_start": { "type": "string" },
        "individual_pronouns_extension_comment": { "type": "string" },
        "comment": { "type": "string" }
    },
    "required": [ "id" ]
}
```

### Sample output
```json
{
  "resourceType": "Patient",
  "id": "123",
  "individual_gender_identity_value_coding_system": "http://terminology.hl7.org/CodeSystem/sex-for-clinical-use",
  "individual_gender_identity_value_coding_code": "specified",
  "individual_gender_identity_value_coding_display": "Specified sex for clinical use",
  "individual_gender_identity_extension_period_start": "2001-05-01",
  "individual_pronouns_value_coding_system": "http://loinc.org",
  "individual_pronouns_value_coding_code": "LA29519-8",
  "individual_pronouns_value_coding_display": "she/her/her/hers/herself",
  "individual_pronouns_extension_period_start": "2001-05-06",
  "individual_pronouns_extension_comment": "Patient transitioned from male to female in 2001.",
  "comment": "Patient transitioned from male to female in 2001, but their driver's license still indicates male."
}
```