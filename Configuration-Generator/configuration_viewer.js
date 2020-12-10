const fs = require('fs')
const path = require('path');


function recursiveCollectProperties(schemaProperties, propertiesGroups, paths, maxLevel) {
    if (maxLevel <= 0) {
        return []
    }
    let properties = [];
    schemaProperties.forEach(function(property) {
        if (property.propertiesGroupRef && property.propertiesGroupRef in propertiesGroups) {
            subPaths = property.path.split('.')

            subPaths.forEach(subPath => {paths.push(subPath)});
            properties = properties.concat(recursiveCollectProperties(propertiesGroups[property.propertiesGroupRef].properties,
                                                                      propertiesGroups,
                                                                      paths,
                                                                      maxLevel - 1));
            subPaths.forEach(subPath => {paths.pop()});
        }
        else {
            let currentProperty = {}
            currentProperty['ColumnName'] = paths.map(subPath => subPath.slice(0,1).toUpperCase() + subPath.slice(1)).join('') + property.name;
            currentProperty['Type'] = property.type;
            properties.push(currentProperty);
        }
    });
    return properties;
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


function showSchema(destination, tableName, maxLevel=3) {
    let propertiesGroups = loadConfigurations(path.join(destination, 'propertiesGroup'), 'propertiesGroupName');

    let configurations = loadConfigurations(destination, 'name');
    
    if (tableName in configurations){
        let properties = recursiveCollectProperties(configurations[tableName].properties, propertiesGroups, [], maxLevel);
        printProperties(tableName, properties);
    }
    else {
        console.log(`Cannot find schema for ${tableName}`);
    }
}


module.exports = {
    loadConfigurations,
    recursiveCollectProperties,
    showSchema
}
