const { program, description, option } = require('commander')
const constants = require('./constants.js');

var configurationGenerator = require('./configuration_generator.js')

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
.action(function(options){
  checkParameters(options, 'generate-config-unrollpath')
  configurationGenerator.publishUnrollConfiguration(options.output, options.unroll, null, options.array, options.overwrite)
});

program.parse(process.argv)