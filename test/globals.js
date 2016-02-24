'use strict';

var uid2 = require('uid2');
var _    = require('lodash');
var path = require('path');

/* eslint vars-on-top: 0 */

global.sinon = require('sinon');
global.chai = require('chai');

global.assert = global.chai.assert;
global.expect = global.chai.expect;
global.should = global.chai.should();

// https://github.com/domenic/chai-as-promised
var chaiAsPromised = require('chai-as-promised');
global.chai.use(chaiAsPromised);

// https://github.com/domenic/sinon-chai
var sinonChai = require('sinon-chai');
global.chai.use(sinonChai);

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
];

global.connect = function (options) {
    options = _.defaultsDeep(options || {}, {
        root: 'TeSt-' + uid2(8),
        statsType: 'riakfs_stats',
        events: false
    });

    return require('../lib/index').create(options);
};
