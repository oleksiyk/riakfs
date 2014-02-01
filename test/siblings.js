"use strict";

/* global before, describe, it, connect */

var Promise = require('bluebird')
var uuid    = require('node-uuid');

describe('Siblings', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    it('mkdir immediately after rmdir (tombstone test)', function() {
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

    it('open immediately after unlink (tombstone test)', function() {
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

    it('file siblings without proper content', function() {
        var id, vclock;
        return riakfs.open('/testFile', 'w').then(function(fd) {
            id = fd.file.id;
            return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                return riakfs.riak.get({
                    bucket: riakfs.filesBucket,
                    key: '/testFile',
                    head: true
                }).then(function(_reply) {
                    vclock = _reply.vclock
                }).then(function() {
                    return riakfs.close(fd)
                })
            })
        })
        .then(function() {
            // make several siblings
            return Promise.map([123, 456, 789], function(len) {
                return riakfs.riak.put({
                    bucket: riakfs.filesBucket,
                    key: '/testFile',
                    vclock: vclock,
                    content: {
                        value: JSON.stringify({
                            id: uuid.v1(),
                            ctime: new Date(),
                            mtime: new Date(),
                            length: len
                        }),
                        content_type: 'application/json'
                    }
                })
            })
        })
        .then(function() {
            return riakfs.stat('/testFile').then(function(stats) {
                stats.should.be.an('object')
                stats.file.id.should.be.eql(id)
                stats.size.should.be.eql(4)
            })
        })
    })

    it('deleted file sibling', function() {
        var id, vclock;
        return riakfs.open('/testDeletedFile', 'w').then(function(fd) {
            id = fd.file.id;
            return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                return riakfs.riak.get({
                    bucket: riakfs.filesBucket,
                    key: '/testDeletedFile',
                    head: true
                }).then(function(_reply) {
                    vclock = _reply.vclock
                }).then(function() {
                    return riakfs.close(fd)
                })
            })
        })
        .then(function() {
            return riakfs.riak.del({
                bucket: riakfs.filesBucket,
                key: '/testDeletedFile',
                vclock: vclock
            })
        })
        .then(function() {
            return riakfs.stat('/testDeletedFile').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
    })

})