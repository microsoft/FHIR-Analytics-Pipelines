# Filter FHIR data in pipeline 

Currently the pipeline supports exporting FHIR data at 2 different scopes:

1. [System](https://hl7.org/Fhir/uv/bulkdata/export/index.html#endpoint---system-level-export): All resources will be exported.
2. [Group](https://hl7.org/Fhir/uv/bulkdata/export/index.html#endpoint---group-of-patients): Patients and associated resources for a particular group resource will be exported. 

Furthermore, you can filter data at a more fine-grained level by specifying parameters `type`, `typeFilter`.

| Parameter | Type | Example | Description |
| --- | --- | --- | --- |
| `type` | string of comma-delimited FHIR resource types | "Condition,MedicationRequest" | Only resources of the specified resource types(s) SHALL be included in the response. |
| `typeFilter` | string of comma-separated list of FHIR REST API queries | "MedicationRequest?status=active,<br>MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z" | `typeFilter` parameter alongside the `type` parameter to further restrict the results of the query. |

## Sample

The following is an sample configuration, in which the user requests to export patient compartment data in a group with the groupId specified.

It requests for `MedicationRequest` and `Condition` resources, where the user would further like to restrict `MedicationRequests` to requests that are `active,` or else `completed` after July 1, 2018.

``` json
{
    "filtering": {
      "scope": "Group",
      "groupId": "f7bc70d0-da32-4f40-9a52-9952707c57db",
      "type": "Condition,MedicationRequest",
      "typeFilter": "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z"
    }
}
```

>Note:

>1. The `Condition` resources is included in `type` but omitted from `typeFilter` because the user intends to request all `Condition` resources without any filters.
>2. There are two filters for `MedicationRequest`, we will process them separately and merge the result.

Here are some additional notes on `type` and `typeFilter`:

1. If this `type` parameter is omitted, will return all supported resources.
   - For system scope, all the FHIR resource types will be returned; 
   - For group scope, all the patient compartment resource types will be returned.
  
2. If a resource type is listed in `type` while has no `typeFilter`, all resources will be included in the output.

3. For resource type with multiple type filters, we will process them separately and merge the result.

   There might be overlap in the output and here we **don't** want deduplicate between the results here as it's very complex. Customers can carefully configure the `typeFilter` to no overlap in output result.

4. Specifying resource type in `typeFilter` but not in `type` is **not permitted.** And an error should be thrown in the parameter validation phase.

5. For group scope, when patient is not in the `type`, we will still process all compartment resources but not returning the patient resources.
