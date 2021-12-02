  
// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

const path = require("path");
const fs = require("fs");
const configurationViewer = require("../configuration_viewer");
const expect = require('chai').expect;


describe('Test configuration viewer can correctly collect properties', function() {
    it(`Given a schema destination folder, can collect required properties for a resource table.`, function(){
        let cdmFolderPath = path.join(__dirname, 'data', 'exampleSchemaFolder');
        let tableName = 'Patient';
        let maxDepth = 3;

        let propertiesGroups = configurationViewer.loadConfigurations(path.join(cdmFolderPath, 'propertiesGroup'), 'propertiesGroupName');
        let configurations = configurationViewer.loadConfigurations(cdmFolderPath, 'name');
        let properties = configurationViewer.getProperties(configurations[tableName], propertiesGroups, maxDepth);
        
        let propertiesGroundTruth = [
            { ColumnName: 'ResourceId', Type: 'string' },
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

    it(`Given a schema destination folder, can collect required properties for a unrollPath table.`, function(){
        let cdmFolderPath = path.join(__dirname, 'data', 'exampleSchemaFolder');
        let tableName = 'PatientName';
        let maxDepth = 3;

        let propertiesGroups = configurationViewer.loadConfigurations(path.join(cdmFolderPath, 'propertiesGroup'), 'propertiesGroupName');
        let configurations = configurationViewer.loadConfigurations(cdmFolderPath, 'name');
        let properties = configurationViewer.getProperties(configurations[tableName], propertiesGroups, maxDepth);

        let propertiesGroundTruth = [
            { ColumnName: 'RowId', Type: 'string' },
            { ColumnName: 'ResourceId', Type: 'string' },
            { ColumnName: 'FhirPath', Type: 'string' },
            { ColumnName: 'ParentPath', Type: 'string' },
            { ColumnName: 'Family', Type: 'string' },
            { ColumnName: 'Given', Type: 'string' },
            { ColumnName: 'PeriodStart', Type: 'string' },
            { ColumnName: 'PeriodEnd', Type: 'string' }
        ];
        // The order is matter. Same with the columns order in generated table
        expect(properties).to.deep.equal(propertiesGroundTruth);
    });

    it(`Set the max level parameter, can stop collecting properties.`, function(){
        let cdmFolderPath = path.join(__dirname, 'data', 'exampleSchemaFolder');
        let tableName = 'Patient';
        let maxDepth = 2;

        let propertiesGroups = configurationViewer.loadConfigurations(path.join(cdmFolderPath, 'propertiesGroup'), 'propertiesGroupName');
        let configurations = configurationViewer.loadConfigurations(cdmFolderPath, 'name');
        let properties = configurationViewer.getProperties(configurations[tableName], propertiesGroups, maxDepth);
        
        let propertiesGroundTruth = [
            { ColumnName: 'ResourceId', Type: 'string' },
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
        let maxDepth = 3;

        let propertiesGroups = {};
        let schemaProperties = [
            {
                path: 'address',
                name: 'Address',
                propertiesGroupRef: "Address"
            }
        ];
        let configuration = {
            properties: schemaProperties,
        }
        expect(function() {
            let properties = configurationViewer.getProperties(configuration, propertiesGroups, maxDepth);
        }).to.throw(`Cannot find propertiesGroup Address`);
    });

    it(`Throw an exception when cannot resolve schema property`, function(){
        let maxDepth = 3;

        let propertiesGroups = {};
        let propertiesMissingType = [
            {
                path: 'address',
                name: 'Address',
            }
        ];
        let configuration = {
            properties: propertiesMissingType
        }
        expect(function() {
            let properties = configurationViewer.getProperties(configuration, propertiesGroups, maxDepth);
        }).to.throw(`Cannot resolve property {"path":"address","name":"Address"} at root:`);
    });
});

