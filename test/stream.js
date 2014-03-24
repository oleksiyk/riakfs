"use strict";

/* global describe, it, testfiles, before, connect */

var Promise = require('bluebird');
var path    = require('path')
var fs      = require('fs')

describe('Stream', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    function copyFileFromFilesystem(from, to){
        return new Promise(function(resolve, reject) {
            var readStream = fs.createReadStream(from)
            var writeStream = riakfs.createWriteStream(to)
            readStream.on('error', reject)
            writeStream.on('error', reject)
            writeStream.on('close', resolve)
            readStream.pipe(writeStream);
        })
    }

    function md5FromStream(filename) {
        return new Promise(function(resolve, reject) {
            var shasum = require('crypto').createHash('md5');
            var s = riakfs.createReadStream(filename);
            s.on('data', function(d) {
                shasum.update(d);
            });
            s.on('end', function() {
                var d = shasum.digest('hex');
                resolve(d)
            });
            s.on('error', reject)
        })
    }

    testfiles.forEach(function(f) {
        it('#writestream should correctly pipe from fs.ReadStream', function() {
            return copyFileFromFilesystem(f.path, '/' + path.basename(f.path)).then(function() {
                return riakfs.stat('/' + path.basename(f.path)).then(function(file) {
                    file.size.should.be.eql(f.size)
                    file.contentType.should.be.eql(f.contentType)
                })
            })
        })
    })

    testfiles.forEach(function(f) {
        it('#readstream should correctly read files', function() {
            return md5FromStream('/' + path.basename(f.path)).should.eventually.be.eql(f.md5)
        })
    })

    it('#writestream should correctly truncate (overwrite) files', function() {
        return copyFileFromFilesystem(testfiles[1].path, '/someimage').then(function() {
            return copyFileFromFilesystem(testfiles[0].path, '/someimage')
        }).then(function() {
            return md5FromStream('/someimage').should.eventually.be.eql(testfiles[0].md5)
        })
    })

})
