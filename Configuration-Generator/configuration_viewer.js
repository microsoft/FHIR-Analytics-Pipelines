const fs = require('fs')
const path = require('path');


function recursiveCollectProperties(schemaProperties, propertiesGroups, paths, maxDepth) {
    maxDepth -= 1
    if (maxDepth == 0) {
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
                                                                      maxDepth));
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


function showSchema(destination, tableName, maxDepth=2) {
    console.log(`{0}, {1}`, destination, tableName)
    let propertiesGroups = loadConfigurations(path.join(destination, 'propertiesGroup'), 'propertiesGroupName');

    let configurations = loadConfigurations(destination, 'name');
    
    if (tableName in configurations){
        let properties = recursiveCollectProperties(configurations[tableName].properties, propertiesGroups, [], maxDepth)
        console.log(properties)
    }
}

module.exports = {showSchema}
