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

    describe('listAll', function () {
        var listfs;
        before(function () {
            return connect().then(function (_riakfs) {
                listfs = _riakfs;
                return Promise.all([
                    listfs.mkdir('/dir1'),
                    listfs.mkdir('/dir2'),
                    listfs.writeFile('/file1', '123'),
                    listfs.writeFile('/人人生而自由，在尊嚴和權利上一律平等。', '123')
                ])
                .then(function () {
                    return Promise.all([
                        listfs.mkdir('/dir1/dir11'),
                        listfs.mkdir('/dir2/dir22'),
                        listfs.writeFile('/dir1/file1', '123'),
                        listfs.writeFile('/dir2/file2', '123'),
                        listfs.writeFile('/dir2/file 2', '123'),
                        listfs.writeFile('/dir2/тест', '123')
                    ]);
                });
            });
        });

        it('should list all files', function () {
            var count = 0;
            return (function _list(marker) {
                return listfs.listAll(3, marker).then(function (result) {
                    count += result.results.length;
                    if (result.continuation) {
                        return _list(result.continuation);
                    }
                    count.should.be.eql(10);
                    return null;
                });
            }());
        });
    });
});
