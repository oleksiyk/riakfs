"use strict";

global.libPath = (process && process.env && process.env.RIAKFS_COV)
    ? '../lib-cov'
    : '../lib';

global.sinon = require("sinon");
global.chai = require("chai");

global.assert = global.chai.assert;
global.should = global.chai.should();

// https://github.com/domenic/mocha-as-promised
require("mocha-as-promised")();

// https://github.com/domenic/chai-as-promised
var chaiAsPromised = require("chai-as-promised");
global.chai.use(chaiAsPromised);

// https://github.com/domenic/sinon-chai
var sinonChai = require("sinon-chai");
global.chai.use(sinonChai);

var path = require('path')

global.testfiles = [
    {
        path: path.dirname(__filename) + '/test-data/image.jpg',
        size: 130566,
        md5: '0b864c06dc35f4fe73afcede3310d8bd',
        contentType: 'image/jpeg'
    }, {
        path: path.dirname(__filename) + '/test-data/image.png',
        size: 1788844,
        md5: '0527806e48c5f6ca0131e36f8ad27c7e',
        contentType: 'image/png'
    }
]

global.connect = function() {
    return require(global.libPath)({
        root: 'test-' + Date.now()
    })
}

