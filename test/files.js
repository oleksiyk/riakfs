"use strict";

/* global before, describe, it, connect, testfiles */

var Promise = require('bluebird');
var fs      = require('fs');
var path    = require('path');
var _       = require('lodash');

describe('Files', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs;
        });
    });

    describe('#open', function() {

        before(function() {
            return riakfs.mkdir('/testDirectory');
        });

        it('should fail for wrong flags - EINVAL', function() {
            return Promise.all([
                riakfs.open('/testfile', 'as').should.be.rejected.and.eventually.have.property('code', 'EINVAL'),
                riakfs.open('/testfile').should.be.rejected.and.eventually.have.property('code', 'EINVAL'),
                riakfs.open('/testfile', '').should.be.rejected.and.eventually.have.property('code', 'EINVAL'),
                riakfs.open('/testfile', 1).should.be.rejected.and.eventually.have.property('code', 'EINVAL'),
            ]);
        });

        ['w', 'w+', 'a', 'a+', 'wx', 'wx+', 'ax', 'ax+'].forEach(function(flag) {
            it('should create new file wih flags=' + flag, function() {
                return riakfs.open('/testnewfile_' + flag, flag)
                    .then(function(fd) {
                        fd.should.be.an('object');
                        fd.should.have.property('flags', flag);
                        fd.should.have.property('file').that.is.an('object');
                        fd.should.have.property('filename', '/testnewfile_' + flag);
                        fd.file.should.have.property('mtime');
                        new Date(fd.file.mtime).should.be.closeTo(new Date(), 500);
                        fd.file.should.have.property('ctime');
                        new Date(fd.file.ctime).should.be.closeTo(new Date(), 500);
                        fd.file.should.have.property('size');
                        fd.file.should.have.property('version', -1);
                    });
            });

            it('should not create file with path name over 4096 characters', function () {
                var _path = '/testnewfile_';
                for(var i = 0; i < 4096; i ++){
                    _path += i;
                }
                return riakfs.open(_path + flag, flag).should.be.rejected.and.eventually.have.property('code', 'ENAMETOOLONG');
            });
        });

        ['wx', 'wx+', 'ax', 'ax+'].forEach(function(flag) {
            it('should fail for existing file with flags=' + flag + ' - EEXIST', function() {
                return riakfs.open('/testnewfile_' + flag, flag).should.be.rejected.and.eventually.have.property('code', 'EEXIST');
            });
        });

        ['r', 'r+'].forEach(function(flag) {
            it('should fail for missing file (or directory) flags=' + flag + ' - ENOENT', function() {
                return riakfs.open('/abracadabra', flag).should.be.rejected.and.eventually.have.property('code', 'ENOENT');
            });
        });

        ['r', 'r+', 'w', 'w+', 'wx', 'wx+', 'a', 'a+', 'ax', 'ax+'].forEach(function(flag) {
            it('should fail for missing path for file (or directory) flags=' + flag + ' - ENOENT', function() {
                return riakfs.open('/abracadabra/abracadabra', flag).should.be.rejected.and.eventually.have.property('code', 'ENOENT');
            });
        });

        ['w', 'w+', 'wx', 'wx+', 'a', 'a+', 'ax', 'ax+'].forEach(function(flag) {
            it('should fail for existing directory with flags=' + flag + ' - EISDIR', function() {
                return riakfs.open('/testDirectory', flag).should.be.rejected.and.eventually.have.property('code', 'EISDIR');
            });
            it('should fail when part of path prefix is not a directory, flags=' + flag + ' - ENOTDIR', function() {
                return riakfs.open('/testnewfile_w/anotherfile', flag).should.be.rejected.and.eventually.have.property('code', 'ENOTDIR');
            });
        });
    });

    describe('#write', function() {

        it('should write data to file', function() {
            return riakfs.open('/testWriteFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('size', 4);
                        fd.file.should.have.property('version', 0);
                        fd.file.should.have.property('contentType', 'text/plain');

                        return riakfs.open('/testWriteFile', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.size);
                            return riakfs.read(fd, buffer, 0, fd.file.size).then(function(length) {
                                length.should.be.eql(fd.file.size);
                                buffer.slice(0,length).toString().should.be.eql('test');
                            });
                        });
                    });
                });
            });

        });

        it('should write data to file by position', function() {
            var fd;
            return riakfs.open('/testWriteFile', 'w').then(function(_fd) {
                fd = _fd;
                return riakfs.write(fd, 'test', 0, 4, null);
            })
            .then(function() {
                return riakfs.write(fd, 'a', 0, 1, 0);
            })
            .then(function() {
                return riakfs.write(fd, 'b', 0, 1, 1);
            })
            .then(function() {
                return riakfs.write(fd, 'c', 0, 1, 2);
            })
            .then(function() {
                return riakfs.write(fd, 'de', 0, 2, 3);
            })
            .then(function() {
                return riakfs.write(fd, ' hello', 0, 6, 5);
            })
            .then(function() {
                return riakfs.close(fd).then(function() {
                    fd.file.should.have.property('size', 11);
                    fd.file.should.have.property('version', 1);
                    fd.file.should.have.property('contentType', 'text/plain');

                    return riakfs.open('/testWriteFile', 'r').then(function(fd) {
                        var buffer = new Buffer(fd.file.size);
                        return riakfs.read(fd, buffer, 0, fd.file.size).then(function(length) {
                            length.should.be.eql(fd.file.size);
                            buffer.slice(0,length).toString().should.be.eql('abcde hello');
                        });
                    });
                });
            });
        });

        it('should write utf8 data to file', function() {
            return riakfs.open('/testWriteFileUtf8', 'w').then(function(fd) {
                return riakfs.write(fd, 'тест', 0, 8, null).then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('size', 8);
                        fd.file.should.have.property('contentType', 'text/plain');

                        return riakfs.open('/testWriteFileUtf8', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.size);
                            return riakfs.read(fd, buffer, 0, fd.file.size).then(function(length) {
                                length.should.be.eql(fd.file.size);
                                buffer.slice(0,length).toString().should.be.eql('тест');
                            });
                        });
                    });
                });
            });
        });

        it('should create file with utf8 chars in the name', function() {
            return riakfs.open('/тестФайл', 'w').then(function(fd) {
                return riakfs.write(fd, 'тест', 0, 8, null).then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('size', 8);
                        fd.file.should.have.property('contentType', 'text/plain');

                        return riakfs.open('/тестФайл', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.size);
                            return riakfs.read(fd, buffer, 0, fd.file.size).then(function(length) {
                                length.should.be.eql(fd.file.size);
                                buffer.slice(0,length).toString().should.be.eql('тест');
                            });
                        });
                    });
                });
            });
        });

        it('should write data to file in several steps', function() {
            return riakfs.open('/testWriteFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 2, null)
                .then(function() {
                    return riakfs.write(fd, 'test', 2, 2, null);
                })
                .then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('size', 4);
                        fd.file.should.have.property('version', 2);
                        fd.file.should.have.property('contentType', 'text/plain');

                        return riakfs.open('/testWriteFile', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.size);
                            return riakfs.read(fd, buffer, 0, fd.file.size).then(function(length) {
                                length.should.be.eql(fd.file.size);
                                buffer.slice(0,length).toString().should.be.eql('test');
                            });
                        });
                    });
                });
            });
        });

        it('should append data to file (flags=a)', function() {
            return riakfs.open('/testWriteFile', 'a').then(function(fd) {
                return riakfs.write(fd, '+test', 0, 5, null)
                .then(function() {
                    return riakfs.close(fd).then(function() {
                        fd.file.should.have.property('size', 9);
                        fd.file.should.have.property('version', 3);
                        fd.file.should.have.property('contentType', 'text/plain');

                        return riakfs.open('/testWriteFile', 'r').then(function(fd) {
                            var buffer = new Buffer(fd.file.size);
                            return riakfs.read(fd, buffer, 0, fd.file.size).then(function(length) {
                                length.should.be.eql(fd.file.size);
                                buffer.slice(0,length).toString().should.be.eql('test+test');
                            });
                        });
                    });
                });
            });
        });
    });

    describe('#read', function() {
        it('should handle empty file', function() {
            return riakfs.open('/testReadFile', 'w').then(function(fd) {
                return riakfs.close(fd);
            })
            .then(function() {
                return riakfs.open('/testReadFile', 'r');
            })
            .then(function(fd) {
                var buffer = new Buffer(100);
                return riakfs.read(fd, buffer, 0, 10, 0).then(function (bytesRead) {
                    bytesRead.should.be.eql(0);
                });
            });
        });

        it('should read data from file', function() {
            return riakfs.open('/testReadFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd);
                });
            })
            .then(function() {
                return riakfs.open('/testReadFile', 'r');
            })
            .then(function(fd) {
                var buffer = new Buffer(4);
                return riakfs.read(fd, buffer, 0, 4, 0).then(function(bytesRead) {
                    buffer.toString().should.be.eql('test');
                    bytesRead.should.be.eql(4);
                });
            });
        });

        it('should read data from file with offset', function() {
            return riakfs.open('/testReadFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd);
                });
            })
            .then(function() {
                return riakfs.open('/testReadFile', 'r');
            })
            .then(function(fd) {
                var buffer = new Buffer(2);
                return riakfs.read(fd, buffer, 0, 2, 2).then(function(bytesRead) {
                    buffer.toString().should.be.eql('st');
                    bytesRead.should.be.eql(2);
                });
            });
        });

        it('should read data incrementaly', function() {
            var fd;
            return riakfs.open('/testReadFile', 'w').then(function(fd) {
                return riakfs.write(fd, 'test', 0, 4, null).then(function() {
                    return riakfs.close(fd);
                });
            })
            .then(function() {
                return riakfs.open('/testReadFile', 'r');
            })
            .then(function(_fd) {
                fd = _fd;
                var buffer = new Buffer(2);
                return riakfs.read(fd, buffer, 0, 2).then(function(length) {
                    buffer.toString().should.be.eql('te');
                    length.should.be.eql(2);
                });
            })
            .then(function() {
                var buffer = new Buffer(20);
                return riakfs.read(fd, buffer, 0, 20).then(function(length) {
                    length.should.be.eql(2);
                    buffer.slice(0,length).toString().should.be.eql('st');
                });
            });
        });
    });

    describe('#writefile', function() {

        testfiles.forEach(function(f) {
            it('should create and write file', function() {
                return Promise.promisify(fs.readFile)(f.path).then(function(data) {
                    return riakfs.writeFile('/' + path.basename(f.path), data);
                })
                .then(function() {
                    return riakfs.stat('/' + path.basename(f.path)).then(function(file) {
                        file.size.should.be.eql(f.size);
                        file.contentType.should.be.eql(f.contentType);
                    });
                });
            });

            it('should overwrite file', function() {
                return Promise.promisify(fs.readFile)(f.path).then(function(data) {
                    return riakfs.writeFile('/' + path.basename(f.path), 'hello')
                    .then(function () {
                        return riakfs.writeFile('/' + path.basename(f.path), data);
                    });
                });
            });
        });

        it('should not fail when data is not buffer or string (undefined)', function() {
            return riakfs.writeFile('/testWrongData');
        });

        it('should not fail when data is not buffer or string', function() {
            return riakfs.writeFile('/testWrongData', 123);
        });
    });

    describe('#readfile', function() {

        testfiles.forEach(function(f) {
            it('should read file into buffer', function() {
                return riakfs.readFile('/' + path.basename(f.path)).then(function(data) {
                    data.length.should.be.eql(f.size);
                    require('crypto').createHash('md5').update(data).digest('hex').should.be.eql(f.md5);
                });
            });
        });
    });

    describe('#unlink', function() {

        before(function() {
            return Promise.all([
                riakfs.mkdir('/unlinkDir'),
                Promise.all(testfiles.map(function(f) {
                    return Promise.promisify(fs.readFile)(f.path).then(function(data) {
                        return riakfs.writeFile('/unlink_' + path.basename(f.path), data);
                    });
                }))
            ]);
        });

        it('should fail for not existing path', function() {
            return riakfs.unlink('/abracadabra').should.be.rejected.and.eventually.have.property('code', 'ENOENT');
        });

        it('should fail for directory', function() {
            return riakfs.unlink('/unlinkDir').should.be.rejected.and.eventually.have.property('code', 'EISDIR');
        });

        testfiles.forEach(function(f) {
            it('should remove file', function() {
                var filename = '/unlink_' + path.basename(f.path);
                return riakfs.stat(filename).then(function(file) {
                    return riakfs.unlink(filename)
                        .then(function() {
                            return Promise.all([
                                riakfs.stat(filename).should.be.rejected.and.eventually.have.property('code', 'ENOENT'),

                                Promise.map(_.range(Math.ceil(file.size / require('../lib/chunk').CHUNK_SIZE)), function(n) {
                                    var key = file.file.id + ':' + n;
                                    return riakfs.riak.get({
                                        bucket: riakfs.chunksBucket,
                                        key: key,
                                        head: true,
                                        type: riakfs.chunksType
                                    }).should.eventually.be.empty;
                                })
                            ]);
                        });
                });
            });
        });
    });

    describe('#copy', function() {

        testfiles.forEach(function(f) {
            it('should copy file', function() {
                var sourceFilename = '/copy1_' + path.basename(f.path);
                var targetFilename = '/copy2_' + path.basename(f.path);

                return Promise.promisify(fs.readFile)(f.path).then(function(data) {
                    return riakfs.writeFile(sourceFilename, data);
                })
                .then(function() {
                    return riakfs.copy(sourceFilename, targetFilename);
                })
                .then(function() {
                    return riakfs.readFile(targetFilename).then(function(data) {
                        data.length.should.be.eql(f.size);
                        require('crypto').createHash('md5').update(data).digest('hex').should.be.eql(f.md5);
                    });
                });
            });
        });

        it('should fail for missing source file', function() {
            return riakfs.copy('/abracadabra1', '/abracadbra2').should.be.rejected.and.eventually.have.property('code', 'ENOENT');
        });
    });

});
