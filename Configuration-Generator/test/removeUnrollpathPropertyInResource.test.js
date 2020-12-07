const path = require("path")
const fs = require("fs")
const configurationGenerator = require("../configuration_generator.js");
const constants =  require("../constants.js")
const utils = require('./util/utils.js');


describe('Test can ignore property in unroll path when creating resource configuration', function() {
    it(`Should not contain unroll path property in published resource configuration`, function(){
        let unrollPath = 'Patient.name';
        let resources = ['Patient'];
        
        let folderName = Math.random().toString(30).substr(2);
        let destination = path.join(__dirname, folderName);
        let arrayOperation = constants.arrayOperations.first;
        let overwrite = false;
        // Publish unroll path configuration for 'Patient.name'
        configurationGenerator.publishUnrollConfiguration(destination, unrollPath, null, arrayOperation, overwrite);
        
        // Publish resource configuration for 'Patient'
        configurationGenerator.publishResourceConfigurations(destination, [resources], null, arrayOperation, overwrite);
        
        let resultFilePath = path.join(destination, `Patient.json`);
        let content = JSON.parse(fs.readFileSync(resultFilePath, 'utf8'));
        deleteFolderRecursive(destination);
        content.properties.forEach(item => {
            if (item.path == 'name') {
                throw 'Property in unroll path not been removed in resource configuration!'
            }
        });
    });
});


describe('Test can update existing resource configuration when creating unroll path configuration', function() {
    it(`Should not contain unroll path property in existing resource configuration`, function(){
        let unrollPath = 'Patient.name';
        let resources = ['Patient'];
        
        let folderName = Math.random().toString(30).substr(2);
        let destination = path.join(__dirname, folderName);
        let arrayOperation = constants.arrayOperations.first;
        let overwrite = false;

        // Publish resource configuration for 'Patient'
        configurationGenerator.publishResourceConfigurations(destination, [resources], null, arrayOperation, overwrite);

        // Publish unroll path configuration for 'Patient.name'
        configurationGenerator.publishUnrollConfiguration(destination, unrollPath, null, arrayOperation, overwrite);
        
        let resultFilePath = path.join(destination, `Patient.json`);
        let content = JSON.parse(fs.readFileSync(resultFilePath, 'utf8'));
        deleteFolderRecursive(destination);
        content.properties.forEach(item => {
            if (item.path == 'name') {
                throw 'Property in unroll path not been removed in resource configuration!'
            }
        });
    });
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