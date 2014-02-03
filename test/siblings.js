"use strict";

/* global before, describe, it, connect, testfiles */

var Promise = require('bluebird')
var uuid    = require('node-uuid');
var path    = require('path');
var fs      = require('fs')

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

    testfiles.forEach(function(f) {
        it('file + file siblings without proper content', function() {
            var id;
            return Promise.promisify(fs.readFile)(f.path).then(function(data) {
                return riakfs.writeFile('/' + path.basename(f.path), data)
            })
            .then(function() {
                return riakfs.stat('/' + path.basename(f.path)).then(function(stats) {
                    id = stats.file.id
                })
            })
            .then(function() {
                // make several siblings
                return Promise.map([0, 123, 456, 789], function(len) {
                    return riakfs.riak.put({
                        bucket: riakfs.filesBucket,
                        key: '/' + path.basename(f.path),
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
                return riakfs.stat('/' + path.basename(f.path)).then(function(stats) {
                    stats.should.be.an('object')
                    stats.file.id.should.be.eql(id)
                    stats.size.should.be.eql(f.size)
                })
            })
        })
    })

    it('file + deleted file sibling', function() {
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

    it('file + empty directory sibling', function() {
        var id, vclock;
        return riakfs.open('/testDirOrFile', 'w').then(function(fd) {
            id = fd.file.id;
            return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                return riakfs.riak.get({
                    bucket: riakfs.filesBucket,
                    key: '/testDirOrFile',
                    head: true
                }).then(function(_reply) {
                    vclock = _reply.vclock
                    return riakfs.close(fd)
                })
            })
        })
        .then(function() {
            return riakfs.riak.put({
                bucket: riakfs.filesBucket,
                key: '/testDirOrFile',
                vclock: vclock,
                content: {
                    value: JSON.stringify({
                        ctime: new Date(),
                        mtime: new Date(),
                        isDirectory: true
                    }),
                    content_type: 'application/json'
                }
            })
        })
        .then(function() {
            return riakfs.stat('/testDirOrFile').then(function(stats) {
                stats.should.be.an('object').and.have.property('file').that.have.property('id')
                stats.file.id.should.be.eql(id)
                stats.size.should.be.eql(4)
            })
        })
    })

})
