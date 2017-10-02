'use strict';

/* global describe, it, connect, before */

var Promise = require('bluebird');

describe('#events', function () {
    var riakfs;

    before(function () {
        return connect({ events: true }).then(function (_riakfs) {
            riakfs = _riakfs;
        });
    });

    describe('new', function () {
        it('file', function (done) {
            riakfs.once('new', function (filename, stats) {
                riakfs.stat(filename).then(function (_stats) {
                    filename.should.be.eql('/test');
                    _stats.should.be.eql(stats);
                }).nodeify(done);
            });
            riakfs.writeFile('/test', 'hello');
        });

        it('directory', function (done) {
            riakfs.once('new', function (filename, stats) {
                riakfs.stat(filename).then(function (_stats) {
                    filename.should.be.eql('/testDir');
                    _stats.should.be.eql(stats);
                }).nodeify(done);
            });
            riakfs.mkdir('/testDir');
        });
    });

    describe('rename', function () {
        it('file', function (done) {
            riakfs.once('rename', function (old, _new, stats) {
                riakfs.stat(_new).then(function (_stats) {
                    old.should.be.eql('/test');
                    _new.should.be.eql('/test1');
                    _stats.should.be.eql(stats);
                }).nodeify(done);
            });
            riakfs.rename('/test', '/test1');
        });

        it('directory', function (done) {
            riakfs.once('rename', function (old, _new, stats) {
                riakfs.stat(_new).then(function (_stats) {
                    old.should.be.eql('/testDir');
                    _new.should.be.eql('/testDir1');
                    _stats.should.be.eql(stats);
                }).nodeify(done);
            });
            riakfs.rename('/testDir', '/testDir1');
        });
    });

    describe('change', function () {
        it('file', function (done) {
            riakfs.once('change', function (filename, stats) {
                riakfs.stat(filename).then(function (_stats) {
                    filename.should.be.eql('/test1');
                    _stats.should.be.eql(stats);
                }).nodeify(done);
            });
            riakfs.appendFile('/test1', ' world');
        });
    });

    describe('updateMeta', function () {
        it('updateMeta', function (done) {
            riakfs.once('updateMeta', function (filename, stats) {
                riakfs.stat(filename).then(function (_stats) {
                    filename.should.be.eql('/test1');
                    _stats.should.be.eql(stats);
                }).nodeify(done);
            });
            riakfs.updateMeta('/test1', { hello: 'world' });
        });

        it('setMeta', function (done) {
            riakfs.once('updateMeta', function (filename, stats) {
                riakfs.stat(filename).then(function (_stats) {
                    filename.should.be.eql('/test1');
                    _stats.should.be.eql(stats);
                }).nodeify(done);
            });
            riakfs.setMeta('/test1', { hello1: 'world1' });
        });
    });

    describe('delete', function () {
        before(function () {
            return riakfs.makeTree('/dir1/dir2/dir3').then(function () {
                return Promise.all([
                    riakfs.writeFile('/dir1/test1', 'hello'),
                    riakfs.writeFile('/dir1/test2', 'hello'),
                    riakfs.writeFile('/dir1/dir2/test1', 'hello'),
                    riakfs.writeFile('/dir1/dir2/test2', 'hello'),
                    riakfs.writeFile('/dir1/dir2/dir3/test1', 'hello'),
                    riakfs.writeFile('/dir1/dir2/dir3/test2', 'hello')
                ]);
            });
        });

        it('file', function (done) {
            var _stats;
            riakfs.once('delete', function (filename, stats) {
                try {
                    filename.should.be.eql('/test1');
                    _stats.should.eql(stats);
                    done();
                } catch (err) {
                    done(err);
                }
            });
            riakfs.stat('/test1').then(function (_s) {
                _stats = _s;
                return riakfs.unlink('/test1');
            });
        });

        it('directory', function (done) {
            var _stats;
            riakfs.once('delete', function (filename, stats) {
                try {
                    filename.should.be.eql('/testDir1');
                    _stats.should.eql(stats);
                    done();
                } catch (err) {
                    done(err);
                }
            });
            riakfs.stat('/testDir1').then(function (_s) {
                _stats = _s;
                return riakfs.rmdir('/testDir1');
            });
        });

        it('rmTree', function (done) {
            var c = 0;
            riakfs.on('delete', function (/*filename*/) {
                if (++c === 9) {
                    done();
                }
            });
            riakfs.rmTree('/dir1');
        });
    });
});
