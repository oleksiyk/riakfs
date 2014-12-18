"use strict";

/* global describe, it, connect, before */

// var Promise = require('bluebird');
// var fs      = require('fs');
// var path    = require('path')

describe('Trash - #unlink', function() {

    var riakfs;

    before(function() {
        return connect({trash: true, events: true}).then(function(_riakfs) {
            riakfs = _riakfs;
        });
    });

    it('should make a copy of deleted file in /.Trash', function() {
        return riakfs.mkdir('/dir1').then(function() {
            return riakfs.writeFile('/dir1/file1', 'hello');
        })
        .then(function () {
            return riakfs.unlink('/dir1/file1');
        })
        .then(function () {
            return riakfs.stat('/dir1/file1').should.be.rejected.and.eventually.have.property('code', 'ENOENT');
        })
        .then(function () {
            return riakfs.readFile('/.Trash/dir1/file1', {encoding: 'utf8'}).then(function(data) {
                data.length.should.be.eql(5);
                data.should.be.a('string').and.eql('hello');
            });
        });
    });

    it('should append sequence suffix if such file already exists in /.Trash', function() {
        return riakfs.writeFile('/dir1/file1', 'hello2')
        .then(function () {
            return riakfs.unlink('/dir1/file1');
        })
        .then(function () {
            return riakfs.stat('/dir1/file1').should.be.rejected.and.eventually.have.property('code', 'ENOENT');
        })
        .then(function () {
            return riakfs.readFile('/.Trash/dir1/file1', {encoding: 'utf8'}).then(function(data) {
                data.length.should.be.eql(5);
                data.should.be.a('string').and.eql('hello');
            });
        })
        .then(function () {
            return riakfs.readFile('/.Trash/dir1/file1.1', {encoding: 'utf8'}).then(function(data) {
                data.length.should.be.eql(6);
                data.should.be.a('string').and.eql('hello2');
            });
        });
    });

    it('should immediately remove file from /.Trash', function() {
        return riakfs.unlink('/.Trash/dir1/file1.1')
        .then(function () {
            return riakfs.readdir('/.Trash/dir1');
        })
        .then(function (list) {
            list.should.contain('file1');
            list.should.not.contain('file1.1');
        });
    });

    it.skip('should emit delete event when file directly unlinked', function (done) {
        riakfs.writeFile('/test1', 'hello').then(function () {
            riakfs.once('delete', function(filename, stats) {
                try {
                    filename.should.be.eql('/test1');
                    stats.should.be.an('object');
                    stats.should.have.property('mtime');
                    stats.should.have.property('ctime');
                    stats.should.have.property('size', 5);
                    stats.should.have.property('contentType');
                    stats.file.should.have.property('version', 0);
                    stats.file.should.have.property('id');
                    done();
                } catch (err){
                    done(err);
                }
            });
            riakfs.unlink('/test1');
        });
    });

    it.skip('should emit delete event when file is moved to Trash', function (done) {
        riakfs.writeFile('/test2', 'hello').then(function () {
            riakfs.once('delete', function(filename, stats) {
                try {
                    filename.should.be.eql('/test2');
                    stats.should.be.an('object');
                    stats.should.have.property('mtime');
                    stats.should.have.property('ctime');
                    stats.should.have.property('size', 5);
                    stats.should.have.property('contentType');
                    stats.file.should.have.property('version', 0);
                    stats.file.should.have.property('id');
                    done();
                } catch (err){
                    done(err);
                }
            });
            riakfs.rename('/test2', '/.Trash/test2');
        });
    });

});
