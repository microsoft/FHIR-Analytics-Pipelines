// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

// TODO: consider using `util` nodule inside node env
const _ = require('lodash');
const fs = require("fs")
const path = require('path')
const interceptors = require('./interceptors');
const MAX_COMPARISION_DEPTH = 100;

const __compareContent = (propPrefix, left, right, depth) => {
    if (depth >= MAX_COMPARISION_DEPTH) {
        if (!_.isEqual(left, right)) {
            throw new Error(`The conversion result has different property: [${propPrefix}]`);
        }
        return true;
    }

    const objectFlag = _.isPlainObject(left) && _.isPlainObject(right);
    const arrayFlag = _.isArray(left) && _.isArray(right);
    
    if (objectFlag) {
        const leftPros = Object.keys(left);
        const rightPros = Object.keys(right);
    
        const totalPros = _.union(leftPros, rightPros);
        const leftDiffs = _.xor(leftPros, totalPros);
        const rightDiffs = _.xor(rightPros, totalPros);
    
        if (!_.isEmpty(leftDiffs)) {
            throw new Error(`The conversion result lacks these properties: [${propPrefix}[${leftDiffs.toString()}]]`);
        }
        else if (!_.isEmpty(rightDiffs)) {
            throw new Error(`The conversion result has these extra properties: [${propPrefix}[${rightDiffs.toString()}]]`);
        }
        else {
            return totalPros.every(prop => __compareContent(`${propPrefix}${prop}.`, left[prop], right[prop], depth + 1));
        }
    }
    // TODO: The array comparision can be done in a finer granularity
    else if (arrayFlag) {
        if (!_.isEqual(left.sort(), right.sort())) {
            throw new Error(`The conversion result has different property: [${propPrefix}Array]`);
        }
        return true;
    }
    else {
        if (_.isEqual(left, right)) {
            return true;
        }
        throw new Error(`The conversion result has different property: [${propPrefix}]`);
    }
};

const compareContent = (content, groundTruth) => {
    if (typeof content !== 'string' || typeof groundTruth !== 'string') {
        throw new Error('The parameters must be both string type.');
    }

    const interceptor = new interceptors.ExtraDynamicFieldInterceptor();
    const left = interceptor.handle(JSON.parse(content));
    const right = interceptor.handle(JSON.parse(groundTruth));
    return __compareContent('', left, right, 0);
};

const getAllFiles = function(dirPath, arrayOfFiles) {
    files = fs.readdirSync(dirPath)
  
    arrayOfFiles = arrayOfFiles || []
  
    files.forEach(function(file) {
      if (fs.statSync(dirPath + "/" + file).isDirectory()) {
        arrayOfFiles = getAllFiles(dirPath + "/" + file, arrayOfFiles)
      } else {
        arrayOfFiles.push(path.join(dirPath, "/", file))
      }
    })
  
    return arrayOfFiles
  }

const deleteFolderRecursive = function(path) {
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

module.exports = {
    MAX_COMPARISION_DEPTH,
    compareContent,
    getAllFiles,
    deleteFolderRecursive
};