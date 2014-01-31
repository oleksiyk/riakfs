"use strict";

/* global before, describe, it, connect */

// var Promise = require('bluebird')
var uuid    = require('node-uuid');

describe('Siblings', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    it('mkdir after rmdir (tombstone test)', function() {
        return riakfs.mkdir('/test').then(function() {
            return riakfs.rmdir('/test').then(function() {
                return riakfs.mkdir('/test').then(function() {
                    return riakfs.riak.get({
                        bucket: riakfs.filesBucket,
                        key: '/test',
                    }).then(function(reply) {
                        reply.content.should.be.an('array').and.have.length(1)
                    })
                })
            })
        })
    })

    it('open after unlink (tombstone test)', function() {
        return riakfs.open('/testFile', 'w').then(function(fd) {
            return riakfs.close(fd)
        })
        .then(function() {
            return riakfs.unlink('/testFile')
        })
        .then(function() {
            return riakfs.open('/testFile', 'w').then(function(fd) {
                return riakfs.close(fd)
            })
        })
        .then(function() {
            return riakfs.riak.get({
                bucket: riakfs.filesBucket,
                key: '/testFile',
            }).then(function(reply) {
                reply.content.should.be.an('array').and.have.length(1)
            })
        })
    })

    it.only('file siblings', function() {
        return riakfs.open('/testFile', 'w').then(function(fd) {
            return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                return riakfs.close(fd)
            })
        })
        .then(function() {
            return riakfs.riak.put({
                bucket: riakfs.filesBucket,
                key: '/testFile',
                content: {
                    value: JSON.stringify({
                        id: uuid.v1(),
                        ctime: new Date(),
                        mtime: new Date(),
                        length: 12
                    }),
                    content_type: 'application/json'
                }
            })
        })
        .then(function() {
            return riakfs.stat('/testFile').then(function(stats) {
                // console.log(stats)
            })
        })
    })

})
