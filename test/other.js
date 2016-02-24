'use strict';

/* global describe, it, connect, before */

var Promise = require('bluebird');
// var fs      = require('fs');
// var path    = require('path')

describe('Other API', function () {
    var riakfs;

    before(function () {
        return connect().then(function (_riakfs) {
            riakfs = _riakfs;
        });
    });

    describe('#exists', function () {
        it('should return true for existing file', function () {
            return riakfs.mkdir('/exists').then(function () {
                return riakfs.exists('/exists')
                    .then(function (exists) {
                        return exists.should.be.true;
                    });
            });
        });

        it('should return false for not existing file', function () {
            return riakfs.exists('/doenotexist').should.eventually.be.false;
        });
    });

    describe('#futimes', function () {
        it('should update atime and mtime for file', function () {
            return riakfs.open('/futimes', 'w').then(function (fd) {
                return riakfs.futimes(fd, new Date(0), new Date(1)).then(function () {
                    return riakfs.stat('/futimes').then(function (stat) {
                        stat.atime.should.be.eql(new Date(0));
                        stat.mtime.should.be.eql(new Date(1));
                    });
                });
            });
        });
    });

    describe('#utimes', function () {
        it('should update atime and mtime for file', function () {
            return riakfs.open('/utimes', 'w').then(function () {
                return riakfs.utimes('/utimes', new Date(10), new Date(11)).then(function () {
                    return riakfs.stat('/utimes').then(function (stat) {
                        stat.atime.should.be.eql(new Date(10));
                        stat.mtime.should.be.eql(new Date(11));
                    });
                });
            });
        });

        it('should only update atime', function () {
            return riakfs.utimes('/utimes', new Date(20), null).then(function () {
                return riakfs.stat('/utimes').then(function (stat) {
                    stat.atime.should.be.eql(new Date(20));
                    stat.mtime.should.be.eql(new Date(11));
                });
            });
        });
    });

    //TODO: check if fstat should update info for opened file
    describe('#fstat', function () {

    });

    describe('global filesystem methods', function () {
        var gfs;
        before(function () {
            return connect({
                trash: true
            })
            .then(function (_riakfs) {
                gfs = _riakfs;
                return Promise.all([
                    gfs.mkdir('/dir1'),
                    gfs.mkdir('/dir2'),
                    gfs.writeFile('/file1', '123'),
                    gfs.writeFile('/人人生而自由，在尊嚴和權利上一律平等。', '123')
                ])
                .then(function () {
                    return Promise.all([
                        gfs.mkdir('/dir1/dir11'),
                        gfs.mkdir('/dir2/dir22'),
                        gfs.writeFile('/dir1/file1', '123'),
                        gfs.writeFile('/dir2/file2', '123'),
                        gfs.writeFile('/dir2/file 2', '123'),
                        gfs.writeFile('/dir2/тест', '123')
                    ]);
                });
            });
        });

        it('listAll should list all files', function () {
            var count = 0;
            return (function _list(marker) {
                return gfs.listAll(3, marker).then(function (result) {
                    count += result.results.length;
                    if (result.continuation) {
                        return _list(result.continuation);
                    }
                    count.should.be.eql(10);
                    return null;
                });
            }());
        });

        it('filesystemStats should return correct storage and file/dir stats', function () {
            return gfs.stat('/').then(function (result) {
                result.should.have.property('file').that.is.an('object');
                result.file.should.have.property('stats').that.is.an('object');
                result.file.stats.should.have.property('storage', 18);
                result.file.stats.should.have.property('files', 6);
            })
            .then(function () {
                return Promise.all([
                    gfs.writeFile('/dir1/file1', '1234'),
                    gfs.appendFile('/dir2/file2', '456'),
                    gfs.unlink('/file1') // goes to Trash
                ]);
            })
            .then(function () {
                return gfs.stat('/').then(function (result) {
                    result.should.have.property('file').that.is.an('object');
                    result.file.should.have.property('stats').that.is.an('object');
                    result.file.stats.should.have.property('storage', 22);
                    result.file.stats.should.have.property('trashStorage', 3);
                    result.file.stats.should.have.property('files', 6);
                });
            })
            .then(function () {
                return gfs.appendFile('/.Trash/file1', '456');
            })
            .then(function () {
                return gfs.stat('/').then(function (result) {
                    result.should.have.property('file').that.is.an('object');
                    result.file.should.have.property('stats').that.is.an('object');
                    result.file.stats.should.have.property('storage', 25);
                    result.file.stats.should.have.property('trashStorage', 6);
                    result.file.stats.should.have.property('files', 6);
                });
            })
            .then(function () {
                return gfs.rename('/.Trash/file1', '/file1-restored');
            })
            .then(function () {
                return gfs.stat('/').then(function (result) {
                    result.should.have.property('file').that.is.an('object');
                    result.file.should.have.property('stats').that.is.an('object');
                    result.file.stats.should.have.property('storage', 25);
                    result.file.stats.should.have.property('trashStorage', 0);
                    result.file.stats.should.have.property('files', 6);
                });
            })
            .then(function () {
                return gfs.unlink('/file1-restored').then(function () {
                    return gfs.unlink('/.Trash/file1-restored');
                });
            })
            .then(function () {
                return gfs.stat('/').then(function (result) {
                    result.should.have.property('file').that.is.an('object');
                    result.file.should.have.property('stats').that.is.an('object');
                    result.file.stats.should.have.property('storage', 19);
                    result.file.stats.should.have.property('trashStorage', 0);
                    result.file.stats.should.have.property('files', 5);
                });
            });
        });
    });
});
