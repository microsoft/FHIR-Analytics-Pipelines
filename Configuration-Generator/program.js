const { program, description, option } = require('commander')
const constants = require('./constants.js');

var configurationGenerator = require('./configuration_generator.js')

program
  .command('generate-config')
  .description('generate configs for input resources, files are put inside local configuration folder')
  .option("-r, --resources <Resources...>", "resources name array")
  .option("-o, --output <Output>", "output folder")
  .option("-a, --array <ArrayOperation>", 'How to handle "array" type for array properties', constants.arrayOperations.aggregate)
  .option("-w, --no-overwrite", "Whether to overwite existed configurations")
  .action(function(options){
    configurationGenerator.publishResourceConfigurations(options.output, options.resources, null, options.array, options.overwrite)
  });

program
.command('generate-config-unrollpath')
.description('generate configs for input unrollpath, files are put inside local configuration folder')
.option("-u, --unroll <UnrollPath>", "unroll path array")
.option("-o, --output <Output>", "output folder")
.option("-a, --array <ArrayOperation>", "How to handle 'array' type for array properties", constants.arrayOperations.aggregate)
.option("-w, --no-overwrite", "Whether to overwite existed configurations")
.action(function(options){
  configurationGenerator.publishUnrollConfiguration(options.output, options.unroll, null, options.array, options.overwrite)
});

program.parse(process.argv)