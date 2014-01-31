"use strict";

/* global before, describe, it, connect, sinon, testfiles */

var Promise = require('bluebird');
var fs      = require('fs');
var path    = require('path')

describe('Files', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    describe('#open', function() {

        before(function() {
            return riakfs.mkdir('/testDirectory')
        })

        it('should fail for wrong flags - EINVAL', function() {
            var cb = sinon.spy(function() {})
            return Promise.all([
                riakfs.open('/testfile', 'as', cb).should.be.rejected.and.eventually.have.property('code', 'EINVAL'),
                riakfs.open('/testfile').should.be.rejected.and.eventually.have.property('code', 'EINVAL'),
                riakfs.open('/testfile', '').should.be.rejected.and.eventually.have.property('code', 'EINVAL'),
                riakfs.open('/testfile', 1).should.be.rejected.and.eventually.have.property('code', 'EINVAL'),
            ]).then(function() {
                cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EINVAL')
            })
        });

        ['w', 'w+', 'a', 'a+', 'wx', 'wx+', 'ax', 'ax+'].forEach(function(flag) {
            it('should create new file wih flags=' + flag, function() {
                var cb = sinon.spy(function() {})
                return riakfs.open('/testnewfile_' + flag, flag, cb)
                    .then(function(fd) {
                        fd.should.be.an('object')
                        fd.should.have.property('flags', flag)
                        fd.should.have.property('file').that.is.an('object')
                        fd.should.have.property('filename', '/testnewfile_' + flag)
                        fd.file.should.have.property('mtime').that.is.closeTo(new Date(), 500)
                        fd.file.should.have.property('ctime').that.is.closeTo(new Date(), 500)
                        fd.file.should.have.property('length')

                        cb.should.have.been.calledWith(null, fd)
                    })
            })
        });

        ['wx', 'wx+', 'ax', 'ax+'].forEach(function(flag) {
            it('should fail for existing file with flags=' + flag + ' - EEXIST', function() {
                var cb = sinon.spy(function() {})
                return riakfs.open('/testnewfile_' + flag, flag, cb).should.be.rejected.and.eventually.have.property('code', 'EEXIST')
                    .then(function() {
                        cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EEXIST')
                    })
            })
        });

        ['r', 'r+'].forEach(function(flag) {
            it('should fail for missing file (or directory) flags=' + flag + ' - ENOENT', function() {
                var cb = sinon.spy(function() {})
                return riakfs.open('/abracadabra', flag, cb).should.be.rejected.and.eventually.have.property('code', 'ENOENT')
                    .then(function() {
                        cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOENT')
                    })
            })
        });

        ['r', 'r+', 'w', 'w+', 'wx', 'wx+', 'a', 'a+', 'ax', 'ax+'].forEach(function(flag) {
            it('should fail for missing path for file (or directory) flags=' + flag + ' - ENOENT', function() {
                var cb = sinon.spy(function() {})
                return riakfs.open('/abracadabra/abracadabra', flag, cb).should.be.rejected.and.eventually.have.property('code', 'ENOENT')
                    .then(function() {
                        cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOENT')
                    })
            })
        });

        ['r', 'r+', 'w', 'w+', 'wx', 'wx+', 'a', 'a+', 'ax', 'ax+'].forEach(function(flag) {
            it('should fail for existing directory with flags=' + flag + ' - EISDIR', function() {
                var cb = sinon.spy(function() {})
                return riakfs.open('/testDirectory', flag, cb).should.be.rejected.and.eventually.have.property('code', 'EISDIR')
                    .then(function() {
                        cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EISDIR')
                    })
            })
            it('should fail when part of path prefix is not a directory, flags=' + flag + ' - ENOTDIR', function() {
                var cb = sinon.spy(function() {})
                return riakfs.open('/testnewfile_w/anotherfile', flag, cb).should.be.rejected.and.eventually.have.property('code', 'ENOTDIR')
                    .then(function() {
                        cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOTDIR')
                    })
            })
        })
    })

    describe('#write', function() {

        it('should write data to file', function() {
            return riakfs.open('/testWriteFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('length', 4)
                        fd.file.should.have.property('contentType', 'text/plain')

                        return riakfs.open('/testWriteFile', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.length)
                            return riakfs.read(fd, buffer, 0, fd.file.length).then(function(length) {
                                length.should.be.eql(fd.file.length)
                                buffer.slice(0,length).toString().should.be.eql('test')
                            })
                        })
                    })
                })
            })

        })

        it('should write data to file by position', function() {
            var fd;
            return riakfs.open('/testWriteFile', 'w').then(function(_fd) {
                fd = _fd;
                return riakfs.write(fd, 'test', 0, 4, null)
            })
            .then(function() {
                return riakfs.write(fd, 'a', 0, 1, 0)
            })
            .then(function() {
                return riakfs.write(fd, 'b', 0, 1, 1)
            })
            .then(function() {
                return riakfs.write(fd, 'c', 0, 1, 2)
            })
            .then(function() {
                return riakfs.write(fd, 'de', 0, 2, 3)
            })
            .then(function() {
                return riakfs.close(fd).then(function() {
                    fd.file.should.have.property('length', 5)
                    fd.file.should.have.property('contentType', 'text/plain')

                    return riakfs.open('/testWriteFile', 'r').then(function(fd) {
                        var buffer = new Buffer(fd.file.length)
                        return riakfs.read(fd, buffer, 0, fd.file.length).then(function(length) {
                            length.should.be.eql(fd.file.length)
                            buffer.slice(0,length).toString().should.be.eql('abcde')
                        })
                    })
                })
            })
        })

        it('should write utf8 data to file', function() {
            return riakfs.open('/testWriteFileUtf8', 'w').then(function(fd) {
                return riakfs.write(fd, 'тест', 0, 8, null).then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('length', 8)
                        fd.file.should.have.property('contentType', 'text/plain')

                        return riakfs.open('/testWriteFileUtf8', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.length)
                            return riakfs.read(fd, buffer, 0, fd.file.length).then(function(length) {
                                length.should.be.eql(fd.file.length)
                                buffer.slice(0,length).toString().should.be.eql('тест')
                            })
                        })
                    })
                })
            })
        })

        it('should write data to file in several steps', function() {
            return riakfs.open('/testWriteFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 2, null)
                .then(function() {
                    return riakfs.write(fd, 'test', 2, 2, null)
                })
                .then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('length', 4)
                        fd.file.should.have.property('contentType', 'text/plain')

                        return riakfs.open('/testWriteFile', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.length)
                            return riakfs.read(fd, buffer, 0, fd.file.length).then(function(length) {
                                length.should.be.eql(fd.file.length)
                                buffer.slice(0,length).toString().should.be.eql('test')
                            })
                        })
                    })
                })
            })
        })

        it('should append data to file (flags=a)', function() {
            return riakfs.open('/testWriteFile', 'a').then(function(fd) {
                return riakfs.write(fd, '+test', 0, 5, null)
                .then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('length', 9)
                        fd.file.should.have.property('contentType', 'text/plain')

                        return riakfs.open('/testWriteFile', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.length)
                            return riakfs.read(fd, buffer, 0, fd.file.length).then(function(length) {
                                length.should.be.eql(fd.file.length)
                                buffer.slice(0,length).toString().should.be.eql('test+test')
                            })
                        })
                    })
                })
            })
        })

        it('should call callback on success', function() {

            var cb = sinon.spy(function() {})

            return riakfs.open('/testWriteFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null, cb).then(function() {
                    return riakfs.close(fd).then(function() {
                        cb.should.have.been.calledWith(null, 4)
                    })
                })
            })
        })

        it('should call callback with error on error', function() {

            var cb = sinon.spy(function() {})
            var data = 'test'

            return riakfs.write(null, data, 0, 4, null, cb).should.be.rejected.and.eventually.have.property('code', 'EBADF')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EBADF')
                })
        })
    })

    describe('#close', function() {
        it('should call callback on success without write', function() {

            var cb = sinon.spy(function() {})

            return riakfs.open('/testCloseFile', 'w').then(function(fd) {
                return riakfs.close(fd, cb).then(function() {
                    cb.should.have.been.calledWith(null)
                })
            })
        })

        it('should call callback on success with write', function() {

            var cb = sinon.spy(function() {})

            return riakfs.open('/testWriteFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd, cb).then(function() {
                        cb.should.have.been.calledWith(null)
                    })
                })
            })
        })

        it('should call callback with error on error', function() {
            var cb = sinon.spy(function() {})
            return riakfs.close(null, cb).should.be.rejected.and.eventually.have.property('code', 'EBADF')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EBADF')
                })
        })
    })

    describe('#read', function() {
        it('should read data from file', function() {
            return riakfs.open('/testReadFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd)
                })
            })
            .then(function() {
                return riakfs.open('/testReadFile', 'r')
            })
            .then(function(fd) {
                var buffer = new Buffer(4)
                return riakfs.read(fd, buffer, 0, 4, 0).then(function() {
                    buffer.toString().should.be.eql('test')
                })
            })
        })

        it('should read data from file with offset', function() {
            return riakfs.open('/testReadFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd)
                })
            })
            .then(function() {
                return riakfs.open('/testReadFile', 'r')
            })
            .then(function(fd) {
                var buffer = new Buffer(2)
                return riakfs.read(fd, buffer, 0, 2, 2).then(function() {
                    buffer.toString().should.be.eql('st')
                })
            })
        })

        it('should read data incrementaly', function() {
            var fd
            return riakfs.open('/testReadFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd)
                })
            })
            .then(function() {
                return riakfs.open('/testReadFile', 'r')
            })
            .then(function(_fd) {
                fd = _fd;
                var buffer = new Buffer(2)
                return riakfs.read(fd, buffer, 0, 2).then(function(length) {
                    buffer.toString().should.be.eql('te')
                    length.should.be.eql(2)
                })
            })
            .then(function() {
                var buffer = new Buffer(20)
                return riakfs.read(fd, buffer, 0, 20).then(function(length) {
                    length.should.be.eql(2)
                    buffer.slice(0,length).toString().should.be.eql('st')
                })
            })
        })
    })

    describe('#writefile', function() {

        testfiles.forEach(function(f) {
            it('should create and write file', function() {
                var cb = sinon.spy(function() {})
                return Promise.promisify(fs.readFile)(f.path).then(function(data) {
                    return riakfs.writeFile('/' + path.basename(f.path), data, cb)
                })
                .then(function() {
                    cb.should.have.been.calledWith(null)
                    return riakfs.stat('/' + path.basename(f.path)).then(function(file) {
                        file.size.should.be.eql(f.size)
                        file.contentType.should.be.eql(f.contentType)
                    })
                })
            })
        })
    })

    describe('#readfile', function() {

        testfiles.forEach(function(f) {
            it('should read file into buffer', function() {
                var cb = sinon.spy(function() {})
                return riakfs.readFile('/' + path.basename(f.path), cb).then(function(data) {
                    data.length.should.be.eql(f.size)
                    require('crypto').createHash('md5').update(data).digest('hex').should.be.eql(f.md5)
                })
            })
        })
    })

    describe('#unlink', function() {

        before(function() {
            return Promise.all([
                riakfs.mkdir('/unlinkDir'),
                Promise.all(testfiles.map(function(f) {
                    return Promise.promisify(fs.readFile)(f.path).then(function(data) {
                        return riakfs.writeFile('/unlink_' + path.basename(f.path), data)
                    })
                }))
            ])
        })

        it('should fail for not existing path', function() {
            var cb = sinon.spy(function() {})
            return riakfs.unlink('/abracadabra', cb).should.be.rejected.and.eventually.have.property('code', 'ENOENT')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOENT')
                })
        })

        it('should fail for directory', function() {
            var cb = sinon.spy(function() {})

            return riakfs.unlink('/unlinkDir', cb).should.be.rejected.and.eventually.have.property('code', 'EISDIR')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EISDIR')
            })
        })

        testfiles.forEach(function(f) {
            it('should remove file', function() {
                var cb = sinon.spy(function() {})
                var filename = '/unlink_' + path.basename(f.path)

                return riakfs.stat(filename).then(function(file) {
                    return riakfs.unlink(filename, cb)
                        .then(function() {
                            cb.should.have.been.calledWith(null)

                            return Promise.all([
                                riakfs.stat(filename).should.be.rejected.and.eventually.have.property('code', 'ENOENT'),

                                riakfs.riak.getIndexAll({
                                    bucket: riakfs.chunksBucket,
                                    index: riakfs.chunksIndex,
                                    qtype: 0,
                                    key: file.file.id
                                }).should.eventually.be.null
                            ])
                        })
                })
            })
        })
    })

})
