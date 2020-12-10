const path = require("path");
const fs = require("fs");
const configurationViewer = require("../configuration_viewer");
const _ = require('lodash');

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
        if (_.isEqual(properties, propertiesGroundTruth)) {
            return true;
        }
        throw new Error(`The conversion result has different properties`);
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
        if (_.isEqual(properties, propertiesGroundTruth)) {
            return true;
        }
        throw new Error(`The conversion result has different properties`);
    });
});
