  
// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

var fs = require('fs')
var path = require('path');
var constants = require('./constants.js');
var generatorUtils = require('./generator_utils.js')

var generalTypeNameRegex = /#\/definitions\/([a-zA-Z0-9_]+)/;
function extractGeneralName(refName) {
    var match = generalTypeNameRegex.exec(refName)
    return match[1]
}

function initOutputFolder(destination, propertiesGroupPath) {
    if (!fs.existsSync(destination)){
        fs.mkdirSync(destination);
    }
    
    if (!fs.existsSync(propertiesGroupPath)){
        fs.mkdirSync(propertiesGroupPath);
    }
}

function getPropertyName(name) {
    return name.slice(0,1).toUpperCase() + name.slice(1);
}

function recursivelyCollectPropertiesGroupTypes(property, visitedPropertyNames, resourceTypeRefs, primitiveTypeRefs, schemaDefinitions) {
    if (!property || visitedPropertyNames.has(property) || constants.excludeProperties.includes(property)) {
        return new Set();
    }
    visitedPropertyNames.add(property);
    let relatedPropertyNames = new Set([property]);
    Object.keys(schemaDefinitions[property].properties).forEach(function(subProperty, _) {
        let typeRef;
        if ('array' == schemaDefinitions[property].properties[subProperty].type) {
            typeRef = schemaDefinitions[property].properties[subProperty].items.$ref
        }
        else if (schemaDefinitions[property].properties[subProperty].$ref) {
            typeRef = schemaDefinitions[property].properties[subProperty].$ref
        }
        
        if (!typeRef) {
            typeRef = constants.reservedPropertyType.string
        }

        if (resourceTypeRefs.includes(typeRef)) {
            typeRef = constants.reservedPropertyType.reference
        }

        let type = extractGeneralName(typeRef)
        if (constants.excludeProperties.includes(type)) {
            // Skip properties with {Extension, ResouceList} type
            return
        }

        if (!primitiveTypeRefs.includes(typeRef)) {
            relatedPropertyNames = new Set([...relatedPropertyNames,
                                            ...recursivelyCollectPropertiesGroupTypes(type, visitedPropertyNames, resourceTypeRefs, primitiveTypeRefs, schemaDefinitions)])
        }
    })
    return relatedPropertyNames;
}

function getRelatedPropertiesGroupTypes(topConfigurations, schema) {
    let resourceTypeRefs = schema.oneOf.map(function(item){return item.$ref})
    let primitiveTypeRefs = constants.primitiveTypes.map(function(item){return item})

    let relatedPropertyNames = new Set();
    let visitedPropertyNames = new Set();
    Object.keys(topConfigurations).forEach(function(type, _) {
        topConfigurations[type].properties.forEach(function(property, _) {
            if (property.propertiesGroupRef) {
                // Recursively Collect potentially appear propertiesGroup types from directly appear propertiesGroup types
                relatedPropertyNames = new Set([...relatedPropertyNames,
                                                ...recursivelyCollectPropertiesGroupTypes(property.propertiesGroupRef, visitedPropertyNames, resourceTypeRefs, primitiveTypeRefs, schema.definitions)])
            }
        })
    })
    return Array.from(relatedPropertyNames)
}

function resolveResourceProperties(resourceType, propertiesInSchema, schema, arrayOperation) {
    var resourceTypeRefs = schema.oneOf.map(function(item){return item.$ref})
    var primitiveTypeRefs = constants.primitiveTypes.map(function(item){return item})

    var properties = []
    Object.keys(propertiesInSchema).forEach(function(propertyName, _) {
        // Skip extension properties
        if (!shouldSkipProperty(propertyName)) {
            var property = {};
            property[constants.configurationConst.path] = propertyName;
            property[constants.configurationConst.name] = getPropertyName(propertyName);
            
            let typeRef;
            if ('array' == propertiesInSchema[propertyName].type) {
                typeRef = arrayOperation==constants.arrayOperations.first?
                          propertiesInSchema[propertyName].items.$ref:
                          constants.reservedPropertyType.array
            }
            else if (propertiesInSchema[propertyName].$ref) {
                typeRef = propertiesInSchema[propertyName].$ref
            }
            
            if (!typeRef) {
                typeRef = constants.reservedPropertyType.string
            }

            if (resourceTypeRefs.includes(typeRef)) {
                typeRef = constants.reservedPropertyType.reference
            }

            if (constants.timeTypes.includes(typeRef)) {
                typeRef = constants.reservedPropertyType.string
            }

            let type = extractGeneralName(typeRef)
            if (constants.excludeProperties.includes(type)) {
                // Skip properties with {Extension, ResouceList} type
                return
            }

            if (primitiveTypeRefs.includes(typeRef)) {
                property[constants.configurationConst.type] = type 
            }
            else {
                property[constants.configurationConst.propertiesGroupRef] = type 
            }
            
            properties.push(property)
        }
    });

    return properties;
}

function resolveUnrollProperties(unrollType, propertyName, schema) {
    var primitiveTypes = constants.primitiveTypes.map(function(item){return extractGeneralName(item)})
    var resourceTypes = schema.oneOf.map(function(item){return extractGeneralName(item.$ref)})
    var property = {};
    property[constants.configurationConst.name] = propertyName
    property[constants.configurationConst.path] = ""
    if (primitiveTypes.includes(unrollType)){
        property[constants.configurationConst.type] = unrollType
    }
    else if(unrollType in resourceTypes){
        property[constants.configurationConst.propertiesGroupRef] = extractGeneralName(constants.reservedPropertyType.reference)
    }
    // propertyGroups
    else {
        property[constants.configurationConst.propertiesGroupRef] = unrollType
    }

    return [property];
}

function generatePropertyGroups(propertyGroupTypes, schema, arrayOperation) {
    if (!propertyGroupTypes) {
        propertyGroupTypes = Object.keys(schema.definitions)
    }
    var resourceTypes = schema.oneOf.map(function(item){return extractGeneralName(item.$ref)})
    var primitiveTypes = constants.primitiveTypes.map(function(item){return extractGeneralName(item)})

    var propertyGroups = [];
    propertyGroupTypes.forEach(function(type) {
        // Ignore resource types, primitive types and other additional pre-defined types
        if (resourceTypes.includes(type) || primitiveTypes.includes(type) || constants.excludeProperties.includes(type)) {
            return;
        }
        var propertyGroup = {};
        propertyGroup[constants.configurationConst.propertiesGroupName] = type
        propertyGroup[constants.configurationConst.properties] = resolveResourceProperties(type, schema.definitions[type].properties, schema, arrayOperation)

        propertyGroups[type] = propertyGroup
    });
    return propertyGroups;
}

function shouldSkipProperty(propertyName) {
    if (propertyName.startsWith('_')) {
        return true
    }

    if (propertyName == 'resourceType' || propertyName == 'extension' || propertyName == 'contained') {
        return true
    }

    return false
}

function generateResourceConfigurations(resourceTypes, schema, arrayOperation) {
    var configurations = []
    resourceTypes.forEach(function(resource_type) {
        var configuration = {};
        configuration[constants.configurationConst.name] = resource_type;
        configuration[constants.configurationConst.resourceType] = resource_type;
        configuration[constants.configurationConst.properties] = resolveResourceProperties(resource_type, schema.definitions[resource_type].properties, schema, arrayOperation)

        configurations[resource_type] = configuration
    });

    return configurations
}

function tryToFindUnrollType(unrollPathSplit, schema) {
    let typeRef, unrollType
    try {
        let subType = unrollPathSplit[0]
        for(i = 0; i < unrollPathSplit.length - 1; i++){
            let subName = unrollPathSplit[i+1]
            let property = schema.definitions[subType].properties;
            if ('array' == property[subName].type) {
                typeRef = property[subName].items.$ref;
            }
            else if (property[subName].$ref){
                typeRef = property[subName].$ref
            }
            else if (property[subName].enum){
                typeRef = constants.reservedPropertyType.code
            }
            else{
                typeRef = constants.reservedPropertyType.string
            }
            //subType = typeRef.split('/')[ typeRef.split('/').length - 1]
            subType = extractGeneralName(typeRef);
        }
        unrollType = subType
    }
    catch(e){
        throw 'Invalid unroll path: ' + unrollPathSplit.join('.') + '\n' + e;
    }
    return unrollType
}

function generateUnrollConfigurations(unrollPaths, schema){
    let unrollConfigurations = [];
    unrollPaths.forEach(function(unrollPath) {
        let unrollPathSplit = unrollPath.split('.');
        let resourceType = unrollPathSplit[0];
        let unrollName = unrollPathSplit.map(path => getPropertyName(path)).join('');
        let propertyName = getPropertyName(unrollPathSplit[unrollPathSplit.length - 1]);
        let unrollType = tryToFindUnrollType(unrollPathSplit, schema);

        let unrollConfiguration = { }
        unrollConfiguration[constants.configurationConst.name] = unrollName;
        unrollConfiguration[constants.configurationConst.resourceType] = resourceType;
        unrollConfiguration[constants.configurationConst.unrollPath] = unrollPathSplit.join('.');
        unrollConfiguration[constants.configurationConst.properties] = resolveUnrollProperties(unrollType, propertyName, schema);
        unrollConfigurations[unrollPath] = unrollConfiguration
    });

    return unrollConfigurations
}

function validateGeneratedResourceConfigurations(resourceConfigurations, destination) {
    let files = fs.readdirSync(destination).filter(file => file.match(RegExp(/.json/)));
    
    // Collect existing unroll paths in destination folder
    let existingUnrollPaths = []
    files.forEach(function(file) {
        filePath = path.join(destination, file);
        content = JSON.parse(fs.readFileSync(filePath));
        if (content.unrollPath) {
            existingUnrollPaths.push(content.unrollPath);
        }
    });
    
    // Remove existing unroll path properties in configurations to be published
    existingUnrollPaths.forEach(function(unrollPath) {
        let subpaths = unrollPath.split('.')

        // Only handle the unroll path property appear in resource configuration
        if (subpaths.length == 2 && resourceConfigurations[subpaths[0]]) {
            resourceConfigurations[subpaths[0]].properties = resourceConfigurations[subpaths[0]].properties.filter(property => property.path != subpaths[1])
        }
    });
    return resourceConfigurations
}

function updateExistingResourceConfigurations(unrollPath, destination) {
    let subPaths = unrollPath.split('.')
    if (subPaths.length != 2) {
        return;
    }
    let files = fs.readdirSync(destination).filter(file => file.match(RegExp(/.json/)));

    // Update the existing resources when create a new unroll path configuration
    files.forEach(function(file) {
        filePath = path.join(destination, file);
        content = JSON.parse(fs.readFileSync(filePath));
        if (!content.unrollPath && content.resourceType == subPaths[0]) {
            content.properties = content.properties.filter(property => property.path != subPaths[1])
            fs.writeFileSync(path.join(filePath), JSON.stringify(content, null, 4))
        }
    });
}

// Export 
function publishUnrollConfiguration(destination, unrollPath, schemaFile, arrayOperation=constants.arrayOperations.first, overwrite=false, validate=false) {
    if (!schemaFile) {
        schemaFile = './fhir.schema.json'
    }
    var schema = require(schemaFile)
    var propertiesGroupPath = path.join(destination, constants.configurationConst.propertyGroupFolder)
    initOutputFolder(destination, propertiesGroupPath)

    // should be only one
    var unrollConfigurations = generateUnrollConfigurations([unrollPath], schema)
    var propertyTypes = getRelatedPropertiesGroupTypes(unrollConfigurations, schema)
    var propertyGroups = generatePropertyGroups(propertyTypes, schema, arrayOperation)

    Object.keys(propertyGroups).forEach(function(groupName, _) {
        if ( overwrite || !fs.existsSync(path.join(propertiesGroupPath, `${groupName}.json`))) {
            fs.writeFileSync(path.join(propertiesGroupPath, `${groupName}.json`), JSON.stringify(propertyGroups[groupName], null, 4))
        }
    })
    Object.keys(unrollConfigurations).forEach(function(unrollPath, _) {
        let unrollFileName = generatorUtils.toCamelCaseString(unrollPath.split('.'))
        fs.writeFileSync(path.join(destination, `${unrollFileName}.json`), JSON.stringify(unrollConfigurations[unrollPath], null, 4))
    })
    if (validate) {
        updateExistingResourceConfigurations(unrollPath, destination)
    }
}

function publishResourceConfigurations(destination, resourceTypes, schemaFile, arrayOperation=constants.arrayOperations.first, overwrite=false, validate=false) {
    if (!schemaFile) {
        schemaFile = './fhir.schema.json'
    }
    var schema = require(schemaFile)
    var propertiesGroupPath = path.join(destination, constants.configurationConst.propertyGroupFolder)
    initOutputFolder(destination, propertiesGroupPath)

    if (!resourceTypes) {
        var resourceTypes = schema.oneOf.map(function(item){return extractGeneralName(item.$ref)})
    }
    var resourceConfigurations = generateResourceConfigurations(resourceTypes, schema, arrayOperation)
    var propertyTypes = getRelatedPropertiesGroupTypes(resourceConfigurations, schema)
    var propertyGroups = generatePropertyGroups(propertyTypes, schema, arrayOperation)

    if (validate) {
        resourceConfigurations = validateGeneratedResourceConfigurations(resourceConfigurations, destination)
    }

    Object.keys(propertyGroups).forEach(function(groupName, _) {
        if ( overwrite || !fs.existsSync(path.join(propertiesGroupPath, `${groupName}.json`))) {
            fs.writeFileSync(path.join(propertiesGroupPath, `${groupName}.json`), JSON.stringify(propertyGroups[groupName], null, 4))
        }
    })
    Object.keys(resourceConfigurations).forEach(function(resourceType, _) {
        fs.writeFileSync(path.join(destination, `${resourceType}.json`), JSON.stringify(resourceConfigurations[resourceType], null, 4))
    })
}

module.exports = { extractGeneralName,
                    publishResourceConfigurations,
                    publishUnrollConfiguration,
                    generateResourceConfigurations,
                    generateUnrollConfigurations,
                    generatePropertyGroups,
                    getRelatedPropertiesGroupTypes
                }
