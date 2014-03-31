"use strict";

/* global describe, it, connect, before */

// var Promise = require('bluebird');
// var fs      = require('fs');
// var path    = require('path')

describe('Trash - #unlink', function() {

    var riakfs;

    before(function() {
        return connect({trash: true, events: true}).then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    it('should make a copy of deleted file in /.Trash', function() {
        return riakfs.mkdir('/dir1').then(function() {
            return riakfs.writeFile('/dir1/file1', 'hello')
        })
        .then(function () {
            return riakfs.unlink('/dir1/file1')
        })
        .then(function () {
            return riakfs.stat('/dir1/file1').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
        .then(function () {
            return riakfs.readFile('/.Trash/dir1/file1', {encoding: 'utf8'}).then(function(data) {
                data.length.should.be.eql(5)
                data.should.be.a('string').and.eql('hello')
            })
        })
    })

    it('should append sequence suffix if such file already exists in /.Trash', function() {
        return riakfs.writeFile('/dir1/file1', 'hello2')
        .then(function () {
            return riakfs.unlink('/dir1/file1')
        })
        .then(function () {
            return riakfs.stat('/dir1/file1').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
        .then(function () {
            return riakfs.readFile('/.Trash/dir1/file1', {encoding: 'utf8'}).then(function(data) {
                data.length.should.be.eql(5)
                data.should.be.a('string').and.eql('hello')
            })
        })
        .then(function () {
            return riakfs.readFile('/.Trash/dir1/file1.1', {encoding: 'utf8'}).then(function(data) {
                data.length.should.be.eql(6)
                data.should.be.a('string').and.eql('hello2')
            })
        })
    })

    it('should immediately remove file from /.Trash', function() {
        return riakfs.unlink('/.Trash/dir1/file1.1')
        .then(function () {
            return riakfs.readdir('/.Trash/dir1')
        })
        .then(function (list) {
            list.should.contain('file1')
            list.should.not.contain('file1.1')
        })
    })

    it('should emit delete event when file is moved to Trash', function (done) {
        riakfs.writeFile('/test1', 'hello').then(function () {
            riakfs.once('delete', function(filename, info) {
                try {
                    filename.should.be.eql('/test1')
                    info.should.be.an('object')
                    info.should.have.property('mtime')
                    info.should.have.property('ctime')
                    info.should.have.property('size', 5)
                    info.should.have.property('contentType')
                    info.should.have.property('version', 0)
                    info.should.have.property('id')
                    done()
                } catch (err){
                    done(err)
                }
            })
            riakfs.unlink('/test1')
        })
    })

})
