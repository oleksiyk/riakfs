"use strict";

/* global describe, it, sinon */

// var Promise = require('bluebird');
// var fs      = require('fs');
// var path    = require('path')

describe('Other API', function() {

    var riakfs = require(global.libPath)({
        root: 'test-' + Date.now()
    })

    describe('#exists', function() {
        it('should return true for existing file', function() {
            var cb = sinon.spy(function() {})
            return riakfs.mkdir('/exists').then(function() {
                return riakfs.exists('/exists', cb)
                    .then(function(exists) {
                        cb.should.have.been.calledWith(null, true)
                        return exists.should.be.true
                    })
            })
        })

        it('should return false for not existing file', function() {
            return riakfs.exists('/doenotexist').should.eventually.be.false
        })
    })

    describe('#futimes', function() {
        it('should update mtime for file', function() {
            return riakfs.open('/futimes', 'w').then(function(fd) {
                return riakfs.futimes(fd, null, new Date(0)).then(function() {
                    return riakfs.stat('/futimes').then(function(stat) {
                        stat.mtime.should.be.eql(new Date(0))
                    })
                })
            })
        })
    })

    describe('#utimes', function() {
        it('should update mtime for file', function() {
            return riakfs.open('/utimes', 'w').then(function() {
                return riakfs.utimes('/utimes', null, new Date(10)).then(function() {
                    return riakfs.stat('/utimes').then(function(stat) {
                        stat.mtime.should.be.eql(new Date(10))
                    })
                })
            })
        })
    })

    //TODO: check if fstat should update info for opened file
    describe('#fstat', function() {

    })

})
