"use strict";

/* global describe, it, connect, before */

var Promise = require('bluebird');

describe('#shared', function() {

    var riakfs1, riakfs2, riakfs3;

    var sharedFs = function(root) {
        switch(root){
            case riakfs1.options.root:
                return Promise.resolve(riakfs1);
            case riakfs2.options.root:
                return Promise.resolve(riakfs2);
            case riakfs3.options.root:
                return Promise.resolve(riakfs3);
            default:
                return Promise.reject('unknown root: ' + root);
        }
    }

    before(function() {
        return Promise.all([
            connect({sharedFs: sharedFs}),
            connect({sharedFs: sharedFs}),
            connect({sharedFs: sharedFs})
        ]).spread(function(_fs1, _fs2, _fs3) {
            riakfs1 = _fs1
            riakfs2 = _fs2
            riakfs3 = _fs3
        })
        .then(function() {
            return Promise.all([
                riakfs1.mkdir('/dir1'),
                riakfs1.mkdir('/dir2'),
            ]).then(function() {
                return Promise.all([
                    riakfs1.mkdir('/dir1/dir2'),
                    riakfs1.writeFile('/dir1/file1', 'hello')
                ])
            })
        })
    })

    it('should create target shared directory', function() {
        return riakfs1.share('/dir1', riakfs2.options.root, 'fs1-dir1').then(function() {
            return riakfs2.stat('/Shared/fs1-dir1').then(function(stats) {
                stats.should.be.an('object')
                stats.isDirectory().should.eql(true)
                stats.isFile().should.eql(false)
            })
        })
    })

    it('#readdir', function() {
        return riakfs2.readdir('/Shared/fs1-dir1').then(function(list) {
            list.should.contain('dir2').and.contain('file1')
        })
    })

    it('#stat', function() {
        return riakfs2.stat('/Shared/fs1-dir1/file1').then(function(stats) {
            stats.should.be.an('object')
            stats.isFile().should.eql(true)
            stats.size.should.be.eql(5)
        })
    })

    it('#exists', function() {
        return riakfs2.exists('/Shared/fs1-dir1/file1').then(function(exists) {
            return exists.should.be.true
        })
    })

    it('#readFile', function() {
        return riakfs2.readFile('/Shared/fs1-dir1/file1', {encoding: 'utf8'}).then(function(data) {
            data.length.should.be.eql(5)
            data.should.be.a('string').and.eql('hello')
        })
    })

    it('#writeFile', function() {
        return riakfs2.writeFile('/Shared/fs1-dir1/file2', 'hello2').then(function() {
            return riakfs1.stat('/dir1/file2').then(function(stats) {
                stats.should.be.an('object')
                stats.isFile().should.eql(true)
                stats.size.should.be.eql(6)
            })
        }).then(function() {
            return riakfs1.readFile('/dir1/file2', {encoding: 'utf8'}).then(function(data) {
                data.length.should.be.eql(6)
                data.should.be.a('string').and.eql('hello2')
            })
        })
    })

    it('#updateMeta', function() {
        var file = {
            filename: '/Shared/fs1-dir1/testFile',
            meta: {
                someKey: 'someValue'
            }
        }

        return riakfs2.writeFile(file.filename, 'test')
        .then(function() {
            return riakfs1.stat('/dir1/testFile').then(function(stats) {
                stats.should.be.an('object')
                stats.size.should.eql(4)
                stats.file.should.not.have.property('meta')
            })
        })
        .then(function() {
            return riakfs2.updateMeta(file.filename, file)
        })
        .then(function() {
            return riakfs1.stat('/dir1/testFile').then(function(stats) {
                stats.should.be.an('object')
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue')
            })
        })
    })

    it('#unlink', function() {
        return riakfs2.unlink('/Shared/fs1-dir1/file2').then(function() {
            return riakfs1.stat('/dir1/file2').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
    })

    it('#mkdir', function() {
        return riakfs2.mkdir('/Shared/fs1-dir1/dir3').then(function() {
            return riakfs1.stat('/dir1/dir3').then(function(stats) {
                stats.should.be.an('object')
                stats.isDirectory().should.eql(true)
            })
        })
    })

    it('#rmdir', function() {
        return riakfs2.rmdir('/Shared/fs1-dir1/dir3').then(function() {
            return riakfs1.stat('/dir1/dir3').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
    })

})
