var fs = require('fs')
var constants = require('./constants.js')

var generalTypeNameRegex = /#\/definitions\/([a-zA-Z0-9_]+)/
function extractTypeName(refName) {
    var match = generalTypeNameRegex.exec(refName)
    return match[1]
}

function toCamelCaseString(path) {
    if (path instanceof String || typeof path === "string") {
        return path.slice(0,1).toUpperCase() + path.slice(1)
    }
    else {
        return path.map(s => s.slice(0,1).toUpperCase() + s.slice(1)).join('')
    }
}

function initOutputFolder(destination, propertiesGroupPath) {
    if (!fs.existsSync(destination)) {
        fs.mkdirSync(destination)
    }
    
    if (!fs.existsSync(propertiesGroupPath)) {
        fs.mkdirSync(propertiesGroupPath)
    }
}

function getTypeFromPropertiesDefinition(property) {
    let typeRef

    if (property.$ref) {
        typeRef = property.$ref
    }
    else if (property.enum) {
        typeRef = constants.reservedPropertyType.code
    }
    else {
        typeRef = constants.reservedPropertyType.string
    }

    return typeRef
}

function getTypeByPath(path, schema) {
    let typeRef, result

    try {
        let subType = path[0]
        for(i = 0; i < path.length - 1; i++){
            let subName = path[i+1]
            let property = schema.definitions[subType].properties;

            if ('array' == property[subName].type) {
                typeRef = getTypeFromPropertiesDefinition(property[subName].items)
            }
            else {
                typeRef = getTypeFromPropertiesDefinition(property[subName])
            }
            
            subType = extractTypeName(typeRef);
        }
        result = subType
    }
    catch(e){
        throw new Error('Invalid path: ' + path.join('.'));
    }

    return result
}

module.exports = { 
    extractTypeName,
    toCamelCaseString,
    initOutputFolder,
    getTypeByPath
}