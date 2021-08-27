  
// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

const primitiveTypes = 
[
    '#/definitions/string',
    '#/definitions/number',
    '#/definitions/decimal',
    '#/definitions/instant',
    '#/definitions/time',
    '#/definitions/boolean',
    '#/definitions/positiveInt',
    '#/definitions/date',
    '#/definitions/dateTime',
    '#/definitions/integer',
    '#/definitions/unsignedInt',
    '#/definitions/base64Binary',
    '#/definitions/canonical',
    '#/definitions/code',
    '#/definitions/id',
    '#/definitions/oid',
    '#/definitions/uri',
    '#/definitions/url',
    '#/definitions/uuid',
    '#/definitions/markdown',
    '#/definitions/xhtml',
    '#/definitions/array'
]

const excludeProperties = [
    'ResourceList',
    'Extension',
]

const reservedPropertyType = {
    reference: '#/definitions/Reference',
    code: '#/definitions/code',
    string: '#/definitions/string',
    array: '#/definitions/array'
}

const timeTypes = [
    '#/definitions/instant',
    '#/definitions/time',
    '#/definitions/date',
    '#/definitions/dateTime'
]


const configurationConst = {
    propertiesGroupName: 'propertiesGroupName',
    propertiesGroupRef: 'propertiesGroupRef',
    properties: 'properties',
    name: 'name',
    type: 'type',
    path: 'path',
    unrollPath: "unrollPath",
    resourceType: 'resourceType',
    propertyGroupFolder: 'PropertiesGroup'
}


const arrayOperations = {
    as_json: "as_json",
    first: "first"
}


module.exports = {
    primitiveTypes: primitiveTypes,
    excludeProperties: excludeProperties,
    reservedPropertyType: reservedPropertyType,
    configurationConst: configurationConst,
    timeTypes: timeTypes,
    arrayOperations: arrayOperations
}