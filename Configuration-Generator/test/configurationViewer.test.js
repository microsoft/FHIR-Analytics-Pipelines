const path = require("path");
const fs = require("fs");
const configurationViewer = require("../configuration_viewer");
const _ = require('lodash');
const expect = require('chai').expect;


describe('Test configuration viewer can correctly collect properties', function() {
    it(`Given a schema destination folder, can collect required properties.`, function(){
        let cdmFolderPath = path.join(__dirname, 'data', 'exampleSchemaFolder');
        let tableName = 'Patient';
        let maxLevel = 3;

        let propertiesGroups = configurationViewer.loadConfigurations(path.join(cdmFolderPath, 'propertiesGroup'), 'propertiesGroupName');
        let configurations = configurationViewer.loadConfigurations(cdmFolderPath, 'name');
        let properties = configurationViewer.recursiveCollectProperties(configurations[tableName].properties, propertiesGroups, [], maxLevel);
        
        let propertiesGroundTruth = [
            { ColumnName: 'Id', Type: 'id' },
            { ColumnName: 'Gender', Type: 'string' },
            { ColumnName: 'NameFamily', Type: 'string' },
            { ColumnName: 'NameGiven', Type: 'string' },
            { ColumnName: 'NamePeriodStart', Type: 'string' },
            { ColumnName: 'NamePeriodEnd', Type: 'string' },
            { ColumnName: 'AddressCity', Type: 'string' },
            { ColumnName: 'AddressDistrict', Type: 'string' },
            { ColumnName: 'AddressCountry', Type: 'string' },
            { ColumnName: 'AddressPeriodStart', Type: 'string' },
            { ColumnName: 'AddressPeriodEnd', Type: 'string' }
        ];
        // The order is matter. Same with the columns order in generated table
        expect(properties).to.deep.equal(propertiesGroundTruth);
    });

    it(`Set the max level parameter, can stop collecting properties.`, function(){
        let cdmFolderPath = path.join(__dirname, 'data', 'exampleSchemaFolder');
        let tableName = 'Patient';
        let maxLevel = 2;

        let propertiesGroups = configurationViewer.loadConfigurations(path.join(cdmFolderPath, 'propertiesGroup'), 'propertiesGroupName');
        let configurations = configurationViewer.loadConfigurations(cdmFolderPath, 'name');
        let properties = configurationViewer.recursiveCollectProperties(configurations[tableName].properties, propertiesGroups, [], maxLevel);
        
        let propertiesGroundTruth = [
            { ColumnName: 'Id', Type: 'id' },
            { ColumnName: 'Gender', Type: 'string' },
            { ColumnName: 'NameFamily', Type: 'string' },
            { ColumnName: 'NameGiven', Type: 'string' },
            { ColumnName: 'AddressCity', Type: 'string' },
            { ColumnName: 'AddressDistrict', Type: 'string' },
            { ColumnName: 'AddressCountry', Type: 'string' },
        ];
        // The order is matter. Same with the columns order in generated table
        expect(properties).to.deep.equal(propertiesGroundTruth);
    });
});


describe('Test configuration viewer throw exceptions', function() {
    it(`Throw an exception when cannot find required propertiesGroup schema`, function(){
        let maxLevel = 3;

        let propertiesGroups = {};
        let schemaProperties = [
            {
                path: 'address',
                name: 'Address',
                propertiesGroupRef: "Address"
            }
        ];
        expect(function() {
            let properties = configurationViewer.recursiveCollectProperties(schemaProperties, propertiesGroups, [], maxLevel);
        }).to.throw(`Cannot find propertiesGroup Address`);
    });

    it(`Throw an exception when cannot resolve schema property`, function(){
        let maxLevel = 3;

        let propertiesGroups = {};
        let propertiesMissingType = [
            {
                path: 'address',
                name: 'Address',
            }
        ];
        expect(function() {
            let properties = configurationViewer.recursiveCollectProperties(propertiesMissingType, propertiesGroups, ['Patient'], maxLevel);
        }).to.throw(`Cannot resolve property {"path":"address","name":"Address"} at Patient`);
    });
});

