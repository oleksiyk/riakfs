"use strict";

/* global describe, it, connect, before */

// var Promise = require('bluebird')

describe('#events', function() {

    var riakfs;

    before(function() {
        return connect({ events: true }).then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    describe('new', function() {

        it('file', function(done) {
            riakfs.once('new', function(filename, info) {
                try {
                    filename.should.be.eql('/test')
                    info.should.be.an('object')
                    info.should.have.property('mtime')
                    info.should.have.property('ctime')
                    info.should.have.property('size', 5)
                    info.should.have.property('contentType')
                    info.should.have.property('version')
                    info.should.have.property('id')
                    done()
                } catch (err){
                    done(err)
                }
            })
            riakfs.writeFile('/test', 'hello')
        })

        it('directory', function(done) {
            riakfs.once('new', function(filename, info) {
                try {
                    filename.should.be.eql('/testDir')
                    info.should.be.an('object')
                    info.should.have.property('mtime')
                    info.should.have.property('ctime')
                    info.should.have.property('isDirectory', true)
                    done()
                } catch (err){
                    done(err)
                }
            })
            riakfs.mkdir('/testDir')
        })

    })

    describe('rename', function() {

        it('file', function(done) {
            riakfs.once('rename', function(old, _new, info) {
                try {
                    old.should.be.eql('/test')
                    _new.should.be.eql('/test1')
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
            riakfs.rename('/test', '/test1')
        })

        it('directory', function(done) {
            riakfs.once('rename', function(old, _new, info) {
                try {
                    old.should.be.eql('/testDir')
                    _new.should.be.eql('/testDir1')
                    info.should.be.an('object')
                    info.should.have.property('mtime')
                    info.should.have.property('ctime')
                    info.should.have.property('isDirectory', true)
                    done()
                } catch (err){
                    done(err)
                }
            })
            riakfs.rename('/testDir', '/testDir1')
        })

    })

    describe('change', function() {
        it('file', function(done) {
            riakfs.once('change', function(filename, info) {
                try {
                    filename.should.be.eql('/test1')
                    info.should.be.an('object')
                    info.should.have.property('mtime')
                    info.should.have.property('ctime')
                    info.should.have.property('size', 11)
                    info.should.have.property('contentType')
                    info.should.have.property('version', 1)
                    info.should.have.property('id')
                    done()
                } catch (err){
                    done(err)
                }
            })
            riakfs.appendFile('/test1', ' world')
        })
    })

    describe('delete', function() {

        it('file', function(done) {
            riakfs.once('delete', function(filename, info) {
                try {
                    filename.should.be.eql('/test1')
                    info.should.be.an('object')
                    info.should.have.property('mtime')
                    info.should.have.property('ctime')
                    info.should.have.property('size', 11)
                    info.should.have.property('contentType')
                    info.should.have.property('version', 1)
                    info.should.have.property('id')
                    done()
                } catch (err){
                    done(err)
                }
            })
            riakfs.unlink('/test1')
        })

        it('directory', function(done) {
            riakfs.once('delete', function(filename, info) {
                try {
                    filename.should.be.eql('/testDir1')
                    info.should.be.an('object')
                    info.should.have.property('mtime')
                    info.should.have.property('ctime')
                    info.should.have.property('isDirectory', true)
                    done()
                } catch (err){
                    done(err)
                }
            })
            riakfs.rmdir('/testDir1')
        })

    })

})
