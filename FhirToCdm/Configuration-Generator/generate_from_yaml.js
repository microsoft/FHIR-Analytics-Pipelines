  
// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

var fs = require('fs')
var path = require('path')
var constants = require('./constants.js')
var schema = require('./fhir.schema.json')
var generatorUtils = require('./generator_utils.js')
const yaml = require('js-yaml')

function addCustomizeProperties(configuration, customProperties) {
    if (!customProperties) {
        return
    }

    customProperties.forEach(function (propertyDefination) {
        var property = {};

        property[constants.configurationConst.path] = propertyDefination.path
        if (propertyDefination.name) {
            property[constants.configurationConst.name] = propertyDefination.name
        }
        else {
            property[constants.configurationConst.name] = generatorUtils.toCamelCaseString(propertyDefination.path.split('.'))
        }

        if (propertyDefination.expression) {
            property['fhirExpression'] = propertyDefination.expression
        }

        if (propertyDefination.type) {
            property[constants.configurationConst.type] = propertyDefination.type
        }
        else {
            property[constants.configurationConst.type] = 'string'
        }

        configuration[constants.configurationConst.properties].push(property)
    })
}

function generatePropertyByUnrollPath(unrollType, propertyName, schema) {
    var primitiveTypes = constants.primitiveTypes.map(function(item){return generatorUtils.extractTypeName(item)})
    var resourceTypes = schema.oneOf.map(function(item){return generatorUtils.extractTypeName(item.$ref)})
    var property = {};

    property[constants.configurationConst.name] = propertyName
    property[constants.configurationConst.path] = ""
    if (primitiveTypes.includes(unrollType)){
        property[constants.configurationConst.type] = unrollType
    }
    else if(unrollType in resourceTypes){
        property[constants.configurationConst.propertiesGroupRef] = generatorUtils.extractTypeName(constants.reservedPropertyType.reference)
    }
    else {
        property[constants.configurationConst.propertiesGroupRef] = unrollType
    }

    return [property];
}

function generatePropertyByPath(resourceType, propertyPath, schema) {
    var primitiveTypes = constants.primitiveTypes.map(function(item){return generatorUtils.extractTypeName(item)})
    var resourceTypes = schema.oneOf.map(function(item){return generatorUtils.extractTypeName(item.$ref)})

    var property = {};
    property[constants.configurationConst.path] = propertyPath
    property[constants.configurationConst.name] = generatorUtils.toCamelCaseString(propertyPath.split('.'))
    pathSplits = [resourceType].concat(propertyPath.split('.'))
    type = generatorUtils.getTypeByPath(pathSplits, schema)
    if (primitiveTypes.includes(type)){
        property[constants.configurationConst.type] = type
    }
    else if(type in resourceTypes){
        property[constants.configurationConst.propertiesGroupRef] = generatorUtils.extractTypeName(constants.reservedPropertyType.reference)
    }
    else {
        property[constants.configurationConst.propertiesGroupRef] = type
    }

    return property
}

function generateUnrollConfigurations(unrollPath, schema){
    let unrollPathSplit = unrollPath.split('.');
    let resourceType = unrollPathSplit[0];
    let name = generatorUtils.toCamelCaseString(unrollPathSplit)
    let propertyName = generatorUtils.toCamelCaseString(unrollPathSplit[unrollPathSplit.length - 1]);
    let unrollType = generatorUtils.getTypeByPath(unrollPathSplit, schema);

    let unrollConfiguration = { }
    unrollConfiguration[constants.configurationConst.name] = name;
    unrollConfiguration[constants.configurationConst.resourceType] = resourceType;
    unrollConfiguration[constants.configurationConst.unrollPath] = unrollPathSplit.join('.');
    unrollConfiguration[constants.configurationConst.properties] = generatePropertyByUnrollPath(unrollType, propertyName, schema);

    return unrollConfiguration
}

function generateResourceConfigurations(resourceType, schema, propertiesList) {
    var configuration = {};
    configuration[constants.configurationConst.name] = resourceType;
    configuration[constants.configurationConst.resourceType] = resourceType;

    properties = []
    propertiesList.forEach(function(path) {
        properties.push(generatePropertyByPath(resourceType, path, schema))
    })
    configuration[constants.configurationConst.properties] = properties

    return configuration
}

function generatePropertiesGroupConfigurations(propertyType, schema, propertiesList) {
    var configuration = {};
    configuration[constants.configurationConst.propertiesGroupName] = propertyType;

    properties = []
    propertiesList.forEach(function(path) {
        properties.push(generatePropertyByPath(propertyType, path, schema))
    })
    configuration[constants.configurationConst.properties] = properties

    return configuration
}

function publishConfigurationsByYaml(destination, resourcesConfigFile, propertiesConfigFile) {
    var propertiesGroupPath = path.join(destination, constants.configurationConst.propertyGroupFolder)
    generatorUtils.initOutputFolder(destination, propertiesGroupPath)

    let resourceConfig = fs.readFileSync(resourcesConfigFile, 'utf8')
    let resources = yaml.safeLoad(resourceConfig)

    if (resources) {
        Object.keys(resources).forEach(function(resourceType, _) {
            resourceObj = resources[resourceType]
            if (resourceObj.unrollPath) {
                resourceObj.unrollPath.forEach(function(unrollPath) {
                    unrollPath = resourceType + '.' + unrollPath
                    let unrollSchema = generateUnrollConfigurations(unrollPath, schema)
                    let unrollFileName = generatorUtils.toCamelCaseString(unrollPath.split('.'))
                    fs.writeFileSync( `${destination}/${unrollFileName}.json`, JSON.stringify(unrollSchema, null, 4))
                })
            }

            resourceSchema = generateResourceConfigurations(resourceType, schema, resourceObj.propertiesByDefault)
            addCustomizeProperties(resourceSchema, resourceObj.customProperties)
            fs.writeFileSync( `${destination}/${resourceType}.json`, JSON.stringify(resourceSchema, null, 4))
        })

        let propertiesConfig = fs.readFileSync(propertiesConfigFile, 'utf8')
        let properties = yaml.safeLoad(propertiesConfig)

        if (properties) {
            Object.keys(properties).forEach(function(propertyName, _) {
                propertyObj = properties[propertyName]

                propertySchema = generatePropertiesGroupConfigurations(propertyName, schema, propertyObj.propertiesByDefault)
                addCustomizeProperties(propertySchema, propertyObj.customProperties)
                fs.writeFileSync( `${destination}/PropertiesGroup/${propertyName}.json`, JSON.stringify(propertySchema, null, 4))
            })
        }
    }
}

const { program, description, option } = require('commander')

program
  .option("-o, --output <Output>", "output folder")
  .option("-r, --resourcesConfigFile <ResourcesConfig>", "Resources config file path.", "./resourcesConfig.yml")
  .option("-p, --propertiesConfigFile <PropertiesConfig>", "Properties group config file path.", "./propertiesGroupConfig.yml")
  .action(function(options){
    publishConfigurationsByYaml(options.output, options.resourcesConfigFile, options.propertiesConfigFile)
  });

if (require.main === module) {
    program.parse(process.argv)
}
else {
    module.exports = { 
        generatePropertyByPath,
        publishConfigurationsByYaml
    }
}