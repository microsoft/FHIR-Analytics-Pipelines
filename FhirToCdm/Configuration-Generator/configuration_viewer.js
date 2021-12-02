  
// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

const fs = require('fs')
const path = require('path');

const baseProperties = {
    resource: [
        {
            ColumnName: 'ResourceId',
            Type: 'string'
        }
    ],
    unrollPath: [
        {
            ColumnName: 'RowId',
            Type: 'string'
        },
        {
            ColumnName: 'ResourceId',
            Type: 'string'
        },
        {
            ColumnName: 'FhirPath',
            Type: 'string'
        },
        {
            ColumnName: 'ParentPath',
            Type: 'string'
        }
    ]
}


function recursiveCollectProperties(schemaProperties, propertiesGroups, paths, maxDepth) {
    if (maxDepth <= 0) {
        return []
    }
    let properties = [];
    schemaProperties.forEach(function(property) {
        if (property.propertiesGroupRef) {
            if (!(property.propertiesGroupRef in propertiesGroups)) {
                throw `Cannot find propertiesGroup ${property.propertiesGroupRef}`;
            }
            subPaths = property.path.split('.')

            subPaths.forEach(subPath => {paths.push(subPath)});
            properties = properties.concat(recursiveCollectProperties(propertiesGroups[property.propertiesGroupRef].properties,
                                                                      propertiesGroups,
                                                                      paths,
                                                                      maxDepth - 1));
            subPaths.forEach(subPath => {paths.pop()});
        }
        else if (property.type) {
            let currentProperty = {}
            currentProperty['ColumnName'] = paths.map(subPath => subPath.slice(0,1).toUpperCase() + subPath.slice(1)).join('') + property.name;
            currentProperty['Type'] = property.type;
            properties.push(currentProperty);
        }
        else {
            throw `Cannot resolve property ${JSON.stringify(property)} at root:${paths.join('.')}`;
        }
    });
    return properties;
}


function getProperties(configuration, propertiesGroups, maxDepth) {
    let properties = configuration.unrollPath?
                        baseProperties.unrollPath:
                        baseProperties.resource;
    let schemaProperties = recursiveCollectProperties(configuration.properties, propertiesGroups, [], maxDepth);
    return properties.concat(schemaProperties);
}


function loadConfigurations(folderPath, nameProperty) {
    let files = fs.readdirSync(folderPath).filter(file => file.match(RegExp(/.json/)));  
    
    let configurations = {};
    files.forEach(function(file) {
        filePath = path.join(folderPath, file);
        content = JSON.parse(fs.readFileSync(filePath));
        configurations[content[nameProperty]] = content;
    });
    return configurations;
}


function printProperties(tableName, properties, minWidth=40) {
    // property: {ColumnName: "type", Type: "type"}
    console.log(`Table Name: ${tableName}`);
    properties.forEach(function(property) {
        let resolvedColumnName = property.ColumnName 
        resolvedColumnName += property.ColumnName.length < minWidth?' '.repeat(minWidth - property.ColumnName.length):'';
        console.log(`- ColumnName: ${resolvedColumnName} Type: ${property.Type}`);
    });
}


function showSchema(destination, tableName, maxDepth=3) {
    let propertiesGroups = loadConfigurations(path.join(destination, 'propertiesGroup'), 'propertiesGroupName');

    let configurations = loadConfigurations(destination, 'name');
    
    if (tableName in configurations){
        let properties = getProperties(configurations[tableName], propertiesGroups, maxDepth);
        printProperties(tableName, properties);
    }
    else {
        console.log(`Cannot find schema for ${tableName}`);
    }
}


module.exports = {
    loadConfigurations,
    getProperties,
    showSchema
}
