  
// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

const { program, description, option } = require('commander')
const constants = require('./constants.js');

var configurationGenerator = require('./configuration_generator.js')
const configurationViewer = require('./configuration_viewer.js')

function checkParameters(options, commandType) {
  if (!(options.array in constants.arrayOperations)){
    throw `Invalid parameters. arrayOperation should be in [${Object.keys(constants.arrayOperations)}]`;
  }
}

program
  .command('generate-config')
  .description('generate configs for input resources, files are put inside local configuration folder')
  .option("-r, --resources <Resources...>", "resources name array")
  .option("-o, --output <Output>", "output folder")
  .option("-a, --array <ArrayOperation>", 'How to handle "array" type for array properties', constants.arrayOperations.first)
  .option("-w, --overwrite", "Whether to overwite existed configurations", false)
  .option("-v, --validate", "Whether to validate generated resource configuration to remove properties appear in unroll configurations", false)
  .action(function(options){
    checkParameters(options, 'generate-config')
    configurationGenerator.publishResourceConfigurations(options.output, options.resources, null, options.array, options.overwrite)
  });

program
.command('generate-config-unrollpath')
.description('generate configs for input unrollpath, files are put inside local configuration folder')
.option("-u, --unroll <UnrollPath>", "unroll path array")
.option("-o, --output <Output>", "output folder")
.option("-a, --array <ArrayOperation>", "How to handle 'array' type for array properties", constants.arrayOperations.first)
.option("-w, --overwrite", "Whether to overwite existed configurations", false)
.option("-v, --validate", "Whether to validate extsing resource configuration to remove properties appear in unroll configurations", false)
.action(function(options){
  checkParameters(options, 'generate-config-unrollpath')
  configurationGenerator.publishUnrollConfiguration(options.output, options.unroll, null, options.array, options.overwrite)
});

program
.command('show-schema')
.description('Show the schema properties')
.option("-d, --destination <destination>", "Destination schema folder")
.option("-t, --tableName <TableName>", "Name of table to show its schema properties")
.option("-maxDepth, --maxDepth <maxLevel>", "Max level to stop recursive collect properties", 3)
.action(function(options){
  configurationViewer.showSchema(options.destination, options.tableName, options.maxDepth);
});

program.parse(process.argv)