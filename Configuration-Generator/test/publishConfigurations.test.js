const path = require("path")
const fs = require("fs")
const configurationGenerator = require("../configuration_generator.js");
const constants =  require("../constants.js")
const utils = require('./util/utils.js');

const resourceCases = ["Organization", "Patient"];
const unrollCase = "Patient.address.city";


describe('Test can write the configurations to target folder', function() {
    it(`Publish resource configuration for ${resourceCases}`, function(){
        let folderName = Math.random().toString(30).substr(2);
        let destination = path.join(__dirname, folderName);
        let arrayOperation = constants.arrayOperations.detail;
        let overwrite = false;

        let results = {};
        let groundTruths = {};
        configurationGenerator.publishResourceConfigurations(destination, resourceCases, null, arrayOperation, overwrite);
        resourceCases.forEach(function(resourceType) {
            let groundTruthFilePath = path.join(__dirname, `./data/resource`, `${resourceType}.json`);
            let groundTruth = fs.readFileSync(groundTruthFilePath, 'utf8');

            let resultFilePath = path.join(destination, `${resourceType}.json`);
            let result = fs.readFileSync(resultFilePath, 'utf8');
            results[resourceType] = result;
            groundTruths[resourceType] = groundTruth;
        })
        deleteFolderRecursive(destination);
        resourceCases.forEach(function(resourceType) {
            utils.compareContent(results[resourceType], groundTruths[resourceType]);
        })
    });

    it(`Publish unroll configuration for ${unrollCase}`, function(){
        let folderName = Math.random().toString(30).substr(2);
        let destination = path.join(__dirname, folderName);
        let arrayOperation = constants.arrayOperations.detail;
        let overwrite = false;

        configurationGenerator.publishUnrollConfiguration(destination, unrollCase, null, arrayOperation, overwrite);
        let unrollName = unrollCase.split('.').join('_')
        let groundTruthFilePath = path.join(__dirname, `./data/unrollpath`, `${unrollName}.json`);
        let groundTruth = fs.readFileSync(groundTruthFilePath, 'utf8');

        let resultFilePath = path.join(destination, `${unrollName}.json`);
        let result = fs.readFileSync(resultFilePath, 'utf8');
        deleteFolderRecursive(destination);
        utils.compareContent(result, groundTruth);
    });
});

describe('Test overwrite for publish configurations', function() {
    it(`Do not overwrite propertiy configurations when overwrite parameter is disable`, function() {
        let folderName = Math.random().toString(30).substr(2);
        let destination = path.join(__dirname, folderName);
        let arrayOperation = constants.arrayOperations.detail;
        let overwrite = false;
        let propertyfilePath = path.join(destination, "PropertiesGroup", "HumanName.json");
    
        configurationGenerator.publishResourceConfigurations(destination, resourceCases, null, arrayOperation, true);
        let statOfPropertyRaw = fs.statSync(propertyfilePath);
        
        configurationGenerator.publishResourceConfigurations(destination, resourceCases, null, arrayOperation, overwrite);
        let statOfPropertyNoOverwrite = fs.statSync(propertyfilePath);
        deleteFolderRecursive(destination);
        if (statOfPropertyNoOverwrite.mtimeMs != statOfPropertyRaw.mtimeMs) {
            throw new Error('Valid overwrite the existed configurations');
        }
    });

    it(`Overwrite resource configurations when overwrite parameter is disable`, function() {
        let folderName = Math.random().toString(30).substr(2);
        let destination = path.join(__dirname, folderName);
        let arrayOperation = constants.arrayOperations.detail;
        let overwrite = false;
        let resourcefilePath = path.join(destination, `${resourceCases[0]}.json`);
    
        configurationGenerator.publishResourceConfigurations(destination, resourceCases, null, arrayOperation, true);
        let statOfResourceRaw = fs.statSync(resourcefilePath);
        
        configurationGenerator.publishResourceConfigurations(destination, resourceCases, null, arrayOperation, overwrite);
        let statOfResourceNoOverwrite = fs.statSync(resourcefilePath);
        deleteFolderRecursive(destination);
        if (statOfResourceNoOverwrite.mtimeMs <= statOfResourceRaw.mtimeMs) {
            throw new Error('Should always overwrite the existed resource configurations');
        }
    });

    
    it(`Overwrite propertiy configurations  when overwrite parameter is enable`, function() {
        let folderName = Math.random().toString(30).substr(2);
        let destination = path.join(__dirname, folderName);
        let arrayOperation = constants.arrayOperations.detail;
        let overwrite = true;
        let propertyfilePath = path.join(destination, "PropertiesGroup", "HumanName.json");
    
        configurationGenerator.publishResourceConfigurations(destination, resourceCases, null, arrayOperation, true);
        let statOfPropertyRaw = fs.statSync(propertyfilePath);

        configurationGenerator.publishResourceConfigurations(destination, resourceCases, null, arrayOperation, overwrite);
        let statOfPropertyOverwrite = fs.statSync(propertyfilePath);
        deleteFolderRecursive(destination);
        if (statOfPropertyOverwrite.mtimeMs <= statOfPropertyRaw.mtimeMs) {
            throw new Error('Should overwrite configurations when overwrite is enable');
        }
    });
    return true;
});

function deleteFolderRecursive(path) {
    if (fs.existsSync(path)) {
        fs.readdirSync(path).forEach(function(file, index){
            var curPath = path + "/" + file;
            if (fs.lstatSync(curPath).isDirectory()) { // recurse
                deleteFolderRecursive(curPath);
            } else { // delete file
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(path);
    }
};