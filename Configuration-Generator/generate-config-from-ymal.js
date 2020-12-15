var fs = require('fs')
var path = require('path')
var constants = require('./constants.js')
var schema = require('./fhir.schema.json')
var generatorUtils = require('./generator-utils.js')
const yaml = require('js-yaml')

function resolveUnrollProperties(unrollType, propertyName, schema) {
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
    unrollConfiguration[constants.configurationConst.properties] = resolveUnrollProperties(unrollType, propertyName, schema);

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

function publishResourceConfigurations(destination) {
    var propertiesGroupPath = path.join(destination, constants.configurationConst.propertyGroupFolder)
    generatorUtils.initOutputFolder(destination, propertiesGroupPath)

    let resourceConfig = fs.readFileSync('./resourcesConfig.yml', 'utf8')
    let resources = yaml.safeLoad(resourceConfig)

    if (resources) {
        Object.keys(resources).forEach(function(resourceType, _) {
            resourceObj = resources[resourceType]
            if (resourceObj.unrollPath) {
                resourceObj.unrollPath.forEach(function(unrollPath) {
                    unrollPath = resourceType + '.' + unrollPath
                    let unrollSchema = generateUnrollConfigurations(unrollPath, schema)
                    fs.writeFileSync( `${destination}/${unrollPath.split('.').join('_')}.json`, JSON.stringify(unrollSchema, null, 4))
                })
            }

            resourceSchema = generateResourceConfigurations(resourceType, schema, resourceObj.propertiesByDefault)
            addCustomizeProperties(resourceSchema, resourceObj.customProperties)
            fs.writeFileSync( `${destination}/${resourceType}.json`, JSON.stringify(resourceSchema, null, 4))
        })

        let propertiesConfig = fs.readFileSync('./propertiesGroupConfig.yml', 'utf8')
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
  .action(function(options){
    publishResourceConfigurations(options.output)
  });

program.parse(process.argv)