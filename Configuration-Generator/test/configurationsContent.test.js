  
// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

var configuration_generator = require("../configuration_generator.js")
var constants =  require("../constants.js")
var generatorUtils = require('../generator_utils.js')

var fs = require("fs")
var path = require("path")
var utils = require('./util/utils.js');

var schema = require('../fhir.schema.json')

var testResources = [
  "Encounter",
  "Observation",
  "Organization",
  "Patient"
]

var testUnrollpaths = [
  "Observation.encounter",
  "Patient.address.city",
  "Patient.address"
]

var testPropertiesGroups = [
  "Address",
  "HumanName",
  "Narrative",
  "Timing"
]

describe('Test generate configurations for resources', function() {
  testResources.forEach(function(resourceType) {
    it(`Generate configuration for \"${resourceType}\" => "${resourceType}.json`, function() {
      let resourceConfiguration = configuration_generator.generateResourceConfigurations([resourceType], schema, constants.arrayOperations.first)[resourceType];
      let groundTruthFilePath = path.join(__dirname, `./data/resource`, `${resourceType}.json`);
      let groundTruth = fs.readFileSync(groundTruthFilePath, 'utf8');
      let result = JSON.stringify(resourceConfiguration, null, 4);
      utils.compareContent(result, groundTruth);
      return true;
    });
  });
});

describe('Test generate configurations for resources when array operation is as_json', function() {
  testResources.forEach(function(resourceType) {
    it(`Generate configuration for \"${resourceType}\" => "${resourceType}.json`, function() {
      let resourceConfiguration = configuration_generator.generateResourceConfigurations([resourceType], schema, constants.arrayOperations.as_json)[resourceType];
      let groundTruthFilePath = path.join(__dirname, `./data/resource`, `${resourceType}.array.json`);
      let groundTruth = fs.readFileSync(groundTruthFilePath, 'utf8');
  
      let result = JSON.stringify(resourceConfiguration, null, 4);  
      utils.compareContent(result, groundTruth);
      return true;
    });
  });
});

describe('Test generate configurations for unrollpaths', function() {
  testUnrollpaths.forEach(function(unrollPath) {
    let tableNameForUnrollPath = generatorUtils.toCamelCaseString(unrollPath.split('.'));
    it(`Generate configuration for \"${unrollPath}\" => "${tableNameForUnrollPath}.json`, function() {
      let unrollPathConfiguration = configuration_generator.generateUnrollConfigurations([unrollPath], schema, constants.arrayOperations.first)[unrollPath];
      let groundTruthFilePath = path.join(__dirname, `./data/unrollpath`, `${tableNameForUnrollPath}.json`);
      let groundTruth = fs.readFileSync(groundTruthFilePath, 'utf8');
      let result = JSON.stringify(unrollPathConfiguration, null, 4);
      utils.compareContent(result, groundTruth);
      return true;
    });
  });
 });

 describe('Test generate configurations for propertyGroups', function() {
   testPropertiesGroups.forEach(function(propertiesGroupType){
    it(`Generate configuration for \"${propertiesGroupType}\" => "${propertiesGroupType}.json`, function() {
      let propertyGroupConfiguration = configuration_generator.generatePropertyGroups([propertiesGroupType], schema, constants.arrayOperations.first)[propertiesGroupType];
      let groundTruthFilePath = path.join(__dirname, `./data/propertiesGroup`, `${propertiesGroupType}.json`);
      let groundTruth = fs.readFileSync(groundTruthFilePath, 'utf8');
      
      let result = JSON.stringify(propertyGroupConfiguration, null, 4);
      utils.compareContent(result, groundTruth);
      return true;
    });
  });
});

describe('Test generate configurations for propertyGroups when array operation is as_json', function() {
  testPropertiesGroups.forEach(function(propertiesGroupType){
    it(`Generate configuration for \"${propertiesGroupType}\" => "${propertiesGroupType}.json`, function() {
      let propertyGroupConfiguration = configuration_generator.generatePropertyGroups([propertiesGroupType], schema, constants.arrayOperations.as_json)[propertiesGroupType];

      let groundTruthFilePath = path.join(__dirname, `./data/propertiesGroup`, `${propertiesGroupType}.array.json`);
      let groundTruth = fs.readFileSync(groundTruthFilePath, 'utf8');
      
      let result = JSON.stringify(propertyGroupConfiguration, null, 4);
      utils.compareContent(result, groundTruth);
      return true;
    });
  });
});