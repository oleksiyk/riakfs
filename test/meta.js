"use strict";

/* global describe, it, connect, before */

var Promise = require('bluebird')

describe.only('#meta', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    it('#open should save meta information with file', function() {
        var file = {
            filename: '/testFile',
            meta: {
                someKey: 'someValue'
            }
        }

        return riakfs.open(file, 'w').then(function(fd) {
            return riakfs.close(fd)
        })
        .then(function() {
            return riakfs.stat(file.filename).then(function(stats) {
                stats.should.be.an('object')
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue')
            })
        })
    })

    it('#writeFile should save meta information with file', function() {
        var file = {
            filename: '/testFile2',
            meta: {
                someKey: 'someValue2'
            }
        }

        return riakfs.writeFile(file, 'test')
        .then(function() {
            return riakfs.stat(file.filename).then(function(stats) {
                stats.should.be.an('object')
                stats.size.should.eql(4)
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue2')
            })
        })
    })

    it('#createWriteStream should save meta information with file', function() {
        var file = {
            filename: '/testFile3',
            meta: {
                someKey: 'someValue3'
            }
        }

        return new Promise(function(resolve, reject) {
            var stream = riakfs.createWriteStream(file)
            stream.on('error', reject)
            stream.on('close', resolve)

            stream.end('test')
        })
        .then(function() {
            return riakfs.stat(file.filename).then(function(stats) {
                stats.should.be.an('object')
                stats.size.should.eql(4)
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue3')
            })
        })
    })

    it('#findAll should find files by custom indexes', function() {
        var file = {
            filename: '/testFile4',
            meta: {
                someKey: 'someValue4'
            },
            indexes: [{
                key: 'test_bin',
                value: 'testValue'
            }]
        }

        return riakfs.open(file, 'w').then(function(fd) {
            return riakfs.close(fd)
        })
        .then(function() {
            return riakfs.findAll({
                index: 'test_bin',
                key: 'testValue'
            }).then(function(search) {
                search.should.be.an('object')
                search.keys.should.be.an('array').and.have.length(1)
                search.keys[0].should.be.eql(file.filename)
            })
        })
    })

})
