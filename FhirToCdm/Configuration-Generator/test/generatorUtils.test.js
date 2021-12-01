const generatorUtils = require("../generator_utils");
const schema = require('../fhir.schema.json');
const expect = require('chai').expect;

describe('Test extractTypeName can correctly extract type name.', function() {
    it(`Given a fhir defination type, can extract general type name.`, function() {
        expect(generatorUtils.extractTypeName('#/definitions/type1')).equals('type1')
        expect(generatorUtils.extractTypeName('#/definitions/Type1')).equals('Type1')
    })
})

describe('Test toCamelCaseString can correctly transform type name.', function() {
    it(`Given a string, can transform to camel case.`, function() {
        expect(generatorUtils.toCamelCaseString('abc')).equals('Abc')
        expect(generatorUtils.toCamelCaseString('ABC')).equals('ABC')
    });
    it(`Given a string array, can transform to camel case.`, function() {
        expect(generatorUtils.toCamelCaseString(['abc', 'bcd'])).equals('AbcBcd')
        expect(generatorUtils.toCamelCaseString(['abc', 'BCD'])).equals('AbcBCD')
        expect(generatorUtils.toCamelCaseString(['abc'])).equals('Abc')
    })
})

describe('Test getTypeByPath can get type by fhir path.', function() {
    it(`Given a valid fhir path, can get fhir type`, function() {
        expect(generatorUtils.getTypeByPath(['Patient', 'name'], schema)).equals('HumanName')
        expect(generatorUtils.getTypeByPath(['Patient', 'name', 'given'], schema)).equals('string')
        expect(generatorUtils.getTypeByPath(['Patient', 'generalPractitioner'], schema)).equals('Reference')
        expect(generatorUtils.getTypeByPath(['HumanName', 'given'], schema)).equals('string')       
    });
})
