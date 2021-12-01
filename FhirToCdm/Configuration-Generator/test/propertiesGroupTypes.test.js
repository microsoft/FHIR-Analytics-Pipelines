  
// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

const constants = require('../constants.js');
const configuration_generator = require("../configuration_generator.js")

const schema = require('../fhir.schema.json');

const testResources = [
  "Observation",
  "CareTeam",
]

const expectPropertiesGroup = [
  'Meta',
  'Element',
  'Coding',
  'Narrative',
  'Identifier',
  'CodeableConcept',
  'Period',
  'Reference',
  'Timing',
  'Timing_Repeat',
  'Duration',
  'Range',
  'Quantity',
  'Ratio',
  'SampledData',
  'Annotation',
  'Observation_ReferenceRange',
  'Observation_Component',
  'CareTeam_Participant',
  'ContactPoint'
]

function getAllPropertiesGroupTypes() {
  let allTypes = Object.keys(schema.definitions);
  let resourceTypes = schema.oneOf.map(function(item){return configuration_generator.extractGeneralName(item.$ref)});
  let primitiveTypes = constants.primitiveTypes.map(function(item){return configuration_generator.extractGeneralName(item)});
  let additionExcludeProperties = ['ResourceList', 'Extension'];

  return allTypes.filter(type => 
    !(resourceTypes.includes(type) || primitiveTypes.includes(type) || additionExcludeProperties.includes(type))
    )
}

describe('Test propertiesGroup types are correct', function() {

  it("Generated all propertiesGroup.", function() {
    let propertiesGroupConfigurations = configuration_generator.generatePropertyGroups(null, schema, constants.arrayOperations.first);
    let expectPropertiesGroupSet = new Set(getAllPropertiesGroupTypes());
    let generatedPropertiesGroupSet = new Set(Object.keys(propertiesGroupConfigurations));
  
    let intersect = new Set([...expectPropertiesGroupSet].filter(x=>generatedPropertiesGroupSet.has(x)));
    if(intersect.size != expectPropertiesGroupSet.size) {
      throw new Error(`Expect types and generate types are different. ${intersect.size}/${expectPropertiesGroupSet.size}`);
    }
    return true;
  });

  it("Generated related propertiesGroup.", function() {
    let resourceConfigurations = configuration_generator.generateResourceConfigurations(testResources, schema, constants.arrayOperations.first);
    let propertyTypes = configuration_generator.getRelatedPropertiesGroupTypes(resourceConfigurations, schema);
    let propertiesGroupConfigurations = configuration_generator.generatePropertyGroups(propertyTypes, schema, constants.arrayOperations.first);
  
    let generatedPropertiesGroupSet = new Set(Object.keys(propertiesGroupConfigurations));
    let expectPropertiesGroupSet = new Set(expectPropertiesGroup);
  
    let intersect=new Set([...expectPropertiesGroupSet].filter(x=>generatedPropertiesGroupSet.has(x)));
    if(intersect.size != expectPropertiesGroupSet.size ||　generatedPropertiesGroupSet.size != expectPropertiesGroupSet.size) {
      throw new Error(`Expect types and generate types are different. ${intersect.size}/${expectPropertiesGroupSet.size}`);
    }
    return true;
  });

  it("Generated related propertiesGroup.", function() {
    let resourceConfigurations = configuration_generator.generateResourceConfigurations(testResources, schema, constants.arrayOperations.first);
    let propertyTypes = configuration_generator.getRelatedPropertiesGroupTypes(resourceConfigurations, schema);
    let propertiesGroupConfigurations = configuration_generator.generatePropertyGroups(propertyTypes, schema, constants.arrayOperations.first);
  
    let generatedPropertiesGroupSet = new Set(Object.keys(propertiesGroupConfigurations));
    let expectPropertiesGroupSet = new Set(expectPropertiesGroup);
  
    let intersect=new Set([...expectPropertiesGroupSet].filter(x=>generatedPropertiesGroupSet.has(x)));
    if(intersect.size != expectPropertiesGroupSet.size || generatedPropertiesGroupSet.size != expectPropertiesGroupSet.size) {
      throw new Error(`Expect types and generate types are different. ${intersect.size}/${expectPropertiesGroupSet.size}`);
    }
    return true;
  });


});
