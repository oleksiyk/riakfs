"use strict";

/* global before, describe, it, connect */

var Promise = require('bluebird')

describe('Directories', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    describe('#mkdir', function() {
        it('should create directory', function() {
            return riakfs.mkdir('/test')
                .then(function() {
                    return riakfs.riak.get({
                        bucket: riakfs.filesBucket,
                        key: '/test'
                    })
                })
                .then(function(reply) {
                    reply.should.be.an('object').and.have.property('content').that.is.an('array')
                    reply.content[0].value.should.have.property('isDirectory', true)
                })
        })

        it('should not create duplicate directory - EEXIST', function() {
            return riakfs.mkdir('/test').should.be.rejected.and.eventually.have.property('code', 'EEXIST')
        })

        it('should not create directories recursively - ENOENT', function() {
            return riakfs.mkdir('/aaa/bbb/ccc').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
    })

    describe('#rmdir', function() {
        it('should remove empty directory', function() {
            return riakfs.rmdir('/test')
                .then(function() {
                    return riakfs.riak.get({
                        bucket: riakfs.filesBucket,
                        key: '/test'
                    })
                })
                .then(function(reply) {
                    reply.should.be.an('object').and.not.have.property('content')
                })
        })

        it('should fail for not existent directory', function() {
            return riakfs.rmdir('/aaa/bbb/ccc').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })

        it('should fail when directory is not empty', function() {
            return riakfs.mkdir('/test')
                .then(function() {
                    return riakfs.open('/test/file', 'w')
                        .then(function(fd) {
                            return riakfs.close(fd)
                        })
                })
                .then(function() {
                    return riakfs.rmdir('/test').should.be.rejected.and.eventually.have.property('code', 'ENOTEMPTY');
                })
        })
    })

    describe('#readdir', function() {
        before(function() {
            return riakfs.mkdir('/readdir').then(function() {
                return Promise.all([
                    riakfs.mkdir('/readdir/directory'),

                    riakfs.writeFile('/readdir/file', 'test')
                ]).then(function() {
                    return Promise.all([
                        riakfs.mkdir('/readdir/directory/level2'),

                        riakfs.open('/readdir/directory/file', 'w').then(function(fd) {
                            return riakfs.close(fd)
                        })
                    ])
                })
            })
        })

        it('should fail for not existent directory', function() {
            return riakfs.readdir('/aaa/bbb/ccc').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })

        it('should return empty array for empty directory', function() {
            return riakfs.mkdir('/readdir2').then(function() {
                return riakfs.readdir('/readdir2').then(function(files) {
                    files.should.be.an('array').and.have.length(0)
                })
            })
        })

        it('should list files and sub-directories in a directory', function() {
            return riakfs.readdir('/readdir').then(function(files) {
                files.should.be.an('array').and.have.length(2)
                files.should.include('directory')
                files.should.include('file')
            })
        })

        it('should list root directory', function() {
            return riakfs.readdir('/').then(function(files) {
                files.should.be.an('array')
                files.should.include('readdir')
                files.should.include('readdir2')
            })
        })

    })

    describe('#makeTree', function() {

        it('should create new directory hierarchy', function() {
            return riakfs.makeTree('/dir1/dir2/dir3').then(function() {
                return riakfs.stat('/dir1/dir2/dir3').then(function(stats) {
                    stats.should.be.an('object')
                    stats.isDirectory().should.eql(true)
                })
            })
        })

        it('should create directory hierarchy with existing prefix', function() {
            return riakfs.makeTree('/dir1/dir2/dir3/dir4/dir5').then(function() {
                return riakfs.stat('/dir1/dir2/dir3/dir4/dir5').then(function(stats) {
                    stats.should.be.an('object')
                    stats.isDirectory().should.eql(true)
                })
            })
        })

        it('should fail when part of prefix is existing file', function() {
            return riakfs.open('/dir1/dir2/file', 'w').then(function(fd) {
                return riakfs.close(fd)
            }).then(function() {
                return riakfs.makeTree('/dir1/dir2/file/dir4/dir5').should.be.rejected.and.eventually.have.property('code', 'ENOTDIR')
            })
        })

    })

})
