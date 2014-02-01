"use strict";

/* global before, describe, it, sinon, connect */

var Promise = require('bluebird')

describe('Directories', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    describe('#mkdir', function() {
        var cb = sinon.spy(function() {})
        it('should create directory', function() {
            return riakfs.mkdir('/test', cb)
                .then(function() {
                    return riakfs.riak.get({
                        bucket: riakfs.filesBucket,
                        key: '/test'
                    })
                })
                .then(function(reply) {
                    reply.should.be.an('object').and.have.property('content').that.is.an('array')
                    reply.content[0].value.should.have.property('isDirectory', true)
                    cb.should.have.been.calledWith(null)
                })
        })

        it('should not create duplicate directory - EEXIST', function() {
            var cb = sinon.spy(function() {})
            return riakfs.mkdir('/test', cb).should.be.rejected.and.eventually.have.property('code', 'EEXIST')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EEXIST')
                })
        })

        it('should not create directories recursively - ENOENT', function() {
            var cb = sinon.spy(function() {})
            return riakfs.mkdir('/aaa/bbb/ccc', cb).should.be.rejected.and.eventually.have.property('code', 'ENOENT')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOENT')
                })
        })
    })

    describe('#rmdir', function() {
        it('should remove empty directory', function() {
            var cb = sinon.spy(function() {})
            return riakfs.rmdir('/test', cb)
                .then(function() {
                    return riakfs.riak.get({
                        bucket: riakfs.filesBucket,
                        key: '/test'
                    })
                })
                .then(function(reply) {
                    reply.should.be.an('object').and.not.have.property('content')
                    cb.should.have.been.calledWith(null)
                })
        })

        it('should fail for not existent directory', function() {
            var cb = sinon.spy(function() {})
            return riakfs.rmdir('/aaa/bbb/ccc', cb).should.be.rejected.and.eventually.have.property('code', 'ENOENT')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOENT')
                })
        })

        it('should fail when directory is not empty', function() {
            var cb = sinon.spy(function() {})
            return riakfs.mkdir('/test')
                .then(function() {
                    return riakfs.open('/test/file', 'w')
                        .then(function(fd) {
                            return riakfs.close(fd)
                        })
                })
                .then(function() {
                    return riakfs.rmdir('/test', cb).should.be.rejected.and.eventually.have.property('code', 'ENOTEMPTY');
                })
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOTEMPTY')
                })
        })
    })

    describe('#readdir', function() {
        before(function() {
            return riakfs.mkdir('/readdir').then(function() {
                return Promise.all([
                    riakfs.mkdir('/readdir/directory'),

                    riakfs.open('/readdir/file', 'w').then(function(fd) {
                        return riakfs.close(fd)
                    })
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
            var cb = sinon.spy(function() {})
            return riakfs.readdir('/aaa/bbb/ccc', cb).should.be.rejected.and.eventually.have.property('code', 'ENOENT')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOENT')
                })
        })

        it('should return empty array for empty directory', function() {
            var cb = sinon.spy(function() {})

            return riakfs.mkdir('/readdir2').then(function() {
                return riakfs.readdir('/readdir2', cb).then(function(files) {
                    files.should.be.an('array').and.have.length(0)
                    cb.should.have.been.calledWith(null, files)
                })
            })
        })

        it('should list files and sub-directories in a directory', function() {
            var cb = sinon.spy(function() {})
            return riakfs.readdir('/readdir', cb).then(function(files) {
                files.should.be.an('array').and.have.length(2)
                files.should.include('directory')
                files.should.include('file')

                cb.should.have.been.calledWith(null, files)
            })
        })

        it('should list root directory', function() {
            var cb = sinon.spy(function() {})
            return riakfs.readdir('/', cb).then(function(files) {
                files.should.be.an('array')
                files.should.include('readdir')
                files.should.include('readdir2')

                cb.should.have.been.calledWith(null, files)
            })
        })

    })

    describe('#makeTree', function() {

        it('should create new directory hierarchy', function() {
            var cb = sinon.spy(function() {})
            return riakfs.makeTree('/dir1/dir2/dir3', cb).then(function() {
                cb.should.have.been.calledWith(null)
                return riakfs.stat('/dir1/dir2/dir3').then(function(stats) {
                    stats.should.be.an('object')
                    stats.isDirectory().should.eql(true)
                })
            })
        })

        it('should create directory hierarchy with existing prefix', function() {
            var cb = sinon.spy(function() {})
            return riakfs.makeTree('/dir1/dir2/dir3/dir4/dir5', cb).then(function() {
                cb.should.have.been.calledWith(null)
                return riakfs.stat('/dir1/dir2/dir3/dir4/dir5').then(function(stats) {
                    stats.should.be.an('object')
                    stats.isDirectory().should.eql(true)
                })
            })
        })

        it('should fail when part of prefix is existing file', function() {
            var cb = sinon.spy(function() {})
            return riakfs.open('/dir1/dir2/file', 'w').then(function(fd) {
                return riakfs.close(fd)
            }).then(function() {
                return riakfs.makeTree('/dir1/dir2/file/dir4/dir5', cb).should.be.rejected.and.eventually.have.property('code', 'ENOTDIR')
            }).then(function() {
                cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOTDIR')
            })
        })

    })

})
