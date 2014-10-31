"use strict";

/* global describe, it, connect, before */

var Promise = require('bluebird');

describe('Quota', function() {

    var riakfs, quotaOK = false;

    before(function() {
        return connect({
            trash: true,
            quotacheck: function () {
                return Promise.resolve(quotaOK);
            }
        }).then(function(_riakfs) {
            riakfs = _riakfs;
        });
    });

    it('should not allow mkdir when quota exceeded', function() {
        quotaOK = false;
        return riakfs.mkdir('/dir1').should.be.rejected.and.eventually.have.property('code', 'EDQUOT');
    });

    it('should allow mkdir when all ok', function() {
        quotaOK = true;
        return riakfs.mkdir('/dir1');
    });

    it('should not allow writeFile when quota exceeded', function() {
        quotaOK = false;
        return riakfs.writeFile('/file1', 'data').should.be.rejected.and.eventually.have.property('code', 'EDQUOT');
    });

    it('should allow writeFile when all ok', function() {
        quotaOK = true;
        return riakfs.writeFile('/file1', 'data');
    });

    it('should allow readFile when quota exceeded', function() {
        quotaOK = false;
        return riakfs.readFile('/file1', {encoding: 'utf8'}).then(function (data) {
            data.should.be.eql('data');
        });
    });

    it('should allow unlink (with Trash feature enabled) when quota exceeded', function() {
        quotaOK = false;
        return riakfs.unlink('/file1');
    });

});
