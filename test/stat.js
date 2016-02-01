'use strict';

/* global describe, it, connect, before */

describe('#stat', function () {
    var riakfs;

    before(function () {
        return connect().then(function (_riakfs) {
            riakfs = _riakfs;
        });
    });

    it('should return valid Stats object for directory', function () {
        return riakfs.mkdir('/testStat').then(function () {
            return riakfs.stat('/testStat').then(function (stats) {
                stats.should.be.an('object');
                stats.should.have.property('mtime').that.is.a('date');
                stats.should.have.property('ctime').that.is.a('date');
                stats.should.have.property('mode');
                stats.should.have.property('uid');
                stats.should.have.property('gid');
                stats.should.respondTo('isDirectory')
                    .and.respondTo('isBlockDevice')
                    .and.respondTo('isCharacterDevice')
                    .and.respondTo('isSymbolicLink')
                    .and.respondTo('isFIFO')
                    .and.respondTo('isSocket');
                stats.isDirectory().should.eql(true);
                stats.isFile().should.eql(false);
            });
        });
    });

    it('should return valid Stats object for /', function () {
        return riakfs.stat('/').then(function (stats) {
            stats.should.be.an('object');
            stats.should.have.property('mtime');
            stats.should.have.property('ctime');
            stats.should.have.property('mode');
            stats.should.have.property('uid');
            stats.should.have.property('gid');
            stats.should.respondTo('isDirectory')
                .and.respondTo('isBlockDevice')
                .and.respondTo('isCharacterDevice')
                .and.respondTo('isSymbolicLink')
                .and.respondTo('isFIFO')
                .and.respondTo('isSocket');
            stats.isDirectory().should.eql(true);
            stats.isFile().should.eql(false);
        });
    });

    it('should return valid Stats object for file', function () {
        return riakfs.open('/testFile', 'w').then(function (fd) {
            return riakfs.write(fd, 'test', 0, 4).then(function () {
                return riakfs.close(fd);
            });
        })
        .then(function () {
            return riakfs.stat('/testFile').then(function (stats) {
                stats.should.be.an('object');
                stats.should.have.property('mtime').that.is.a('date');
                stats.should.have.property('ctime').that.is.a('date');
                stats.should.have.property('mode');
                stats.should.have.property('uid');
                stats.should.have.property('gid');
                stats.should.respondTo('isDirectory')
                    .and.respondTo('isBlockDevice')
                    .and.respondTo('isCharacterDevice')
                    .and.respondTo('isSymbolicLink')
                    .and.respondTo('isFIFO')
                    .and.respondTo('isSocket');
                stats.isDirectory().should.eql(false);
                stats.isFile().should.eql(true);
                stats.size.should.eql(4);
            });
        });
    });

    it('should fail for not existing path - ENOENT', function () {
        return riakfs.stat('/djhjdhjehw/sjhsjhsj').should.be.rejected.and.eventually.have.property('code', 'ENOENT');
    });
});
