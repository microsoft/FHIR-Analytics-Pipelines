const path = require("path")
const fs = require("fs")
const configurationGenerator = require("../generate_from_yaml.js");
const constants =  require("../constants.js")
const utils = require('./util/utils.js');
const generatorUtils = require('../generator_utils.js')
const schema = require('../fhir.schema.json');
const expect = require('chai').expect;

const resourceCases = ["Organization", "Patient"];
const unrollCase = "Patient.address.city";

describe('Test generatePropertyByUnrollPath', function() {
    it("Test generatePropertyByUnrollPath for Patient.name", function() {
        let result = configurationGenerator.generatePropertyByPath("Patient", "name", schema)

        expect(result[constants.configurationConst.path]).equals('name')
        expect(result[constants.configurationConst.name]).equals('Name')
        expect(result[constants.configurationConst.propertiesGroupRef]).equals("HumanName")
    })

    it("Test generatePropertyByUnrollPath for Patient.name.given", function() {
        let result = configurationGenerator.generatePropertyByPath("Patient", "name.given", schema)

        expect(result[constants.configurationConst.path]).equals('name.given')
        expect(result[constants.configurationConst.name]).equals('NameGiven')
        expect(result[constants.configurationConst.type]).equals("string")
    })
})

describe('Test publishConfigurationsByYaml', function() {
    it("Test publishConfigurationsByYaml for test yaml config", function() {
        let folderName = Math.random().toString(30).substr(2);
        let destination = path.join(__dirname, folderName);
        let expectOutputs = path.join(__dirname, `./data/YamlTestOutputs`);

        let resourcesConfigFilePath = path.join(__dirname, `./data/YamlTestConfigs`, `resourcesConfig.yml`);
        let propertiesConfigFilePath = path.join(__dirname, `./data/YamlTestConfigs`, `propertiesGroupConfig.yml`);

        try {
            configurationGenerator.publishConfigurationsByYaml(destination, resourcesConfigFilePath, propertiesConfigFilePath)

            let resultFiles = utils.getAllFiles(destination)
            let expectFiles = utils.getAllFiles(expectOutputs)

            expect(resultFiles.length).equals(expectFiles.length)

            for (let i = 0; i < resultFiles.length; ++i) {
                let result = fs.readFileSync(resultFiles[i], 'utf8');
                let expect = fs.readFileSync(expectFiles[i], 'utf8');
                
                utils.compareContent(result, expect);
            }
        }
        finally {
            utils.deleteFolderRecursive(destination)
        }
    })
})